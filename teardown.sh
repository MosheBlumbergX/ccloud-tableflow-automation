#!/usr/bin/env bash
# Remove the Kubernetes workloads, disable any Tableflow topics the watcher
# turned on, and optionally destroy the Terraform-managed infrastructure.
set -uo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$DIR/terraform"

DESTROY_INFRA="${DESTROY_INFRA:-false}"
UNINSTALL_CFK="${UNINSTALL_CFK:-true}"
DELETE_NAMESPACE="${DELETE_NAMESPACE:-false}"

step() { printf '\n\033[1m=== %s ===\033[0m\n' "$1"; }
info() { printf '  %s\n' "$1"; }

if [[ -f "$DIR/demo.env" ]]; then
  # shellcheck disable=SC1091
  source "$DIR/demo.env"
fi

PREFIX="${PREFIX:-moshe}"
NS="${NS:-${PREFIX}-confluent}"
HELM_RELEASE="${HELM_RELEASE:-${PREFIX}-operator}"
SECRET_KAFKA="${SECRET_KAFKA:-${PREFIX}-cloud-rest-access}"
SECRET_SR="${SECRET_SR:-${PREFIX}-cloud-sr-access}"
SECRET_TABLEFLOW="${SECRET_TABLEFLOW:-${PREFIX}-tableflow-api-access}"
SCRIPT_CONFIGMAP="${SCRIPT_CONFIGMAP:-${PREFIX}-tableflow-watcher-script}"
WATCHER_NAME="${WATCHER_NAME:-${PREFIX}-tableflow-watcher}"

info "prefix: $PREFIX, namespace: $NS"

# --- 1. Stop the watcher before anything else -----------------------------------
# Otherwise it re-enables Tableflow on the topics we are about to disable.
step "Stopping the watcher"
kubectl delete deployment "$WATCHER_NAME" -n "$NS" --ignore-not-found

# --- 2. Disable Tableflow topics -------------------------------------------------
# The watcher enables these outside Terraform, and the Azure provider
# integration cannot be destroyed while a Tableflow topic still references it.
step "Disabling Tableflow topics"
if [[ -n "${ENV_ID:-}" && -n "${CLUSTER_ID:-}" ]] && terraform -chdir="$TF_DIR" output -raw tableflow_api_key >/dev/null 2>&1; then
  TF_KEY="$(terraform -chdir="$TF_DIR" output -raw tableflow_api_key)"
  TF_SECRET="$(terraform -chdir="$TF_DIR" output -raw tableflow_api_secret)"
  BASE="https://api.confluent.cloud/tableflow/v1/tableflow-topics"
  SCOPE="environment=${ENV_ID}&spec.kafka_cluster=${CLUSTER_ID}"

  TOPICS="$(curl -sf -u "$TF_KEY:$TF_SECRET" "$BASE?$SCOPE&page_size=100" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for item in data.get('data', []):
    name = item.get('spec', {}).get('display_name')
    if name:
        print(name)
" 2>/dev/null)"

  if [[ -z "$TOPICS" ]]; then
    info "no Tableflow topics found"
  else
    for topic in $TOPICS; do
      code="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE \
        -u "$TF_KEY:$TF_SECRET" "$BASE/${topic}?$SCOPE")"
      info "disabled $topic (HTTP $code)"
    done
    # Deletion is asynchronous, and the provider integration stays busy until
    # it finishes.
    info "waiting for Tableflow cleanup to settle..."
    sleep 30
  fi
else
  info "skipped: no Terraform outputs or demo.env available"
fi

# --- 3. Kubernetes resources ------------------------------------------------------
step "Kubernetes resources"
kubectl delete -f "$DIR/manifests/tableflow-watcher.yaml" --ignore-not-found
kubectl delete configmap "$SCRIPT_CONFIGMAP" -n "$NS" --ignore-not-found
kubectl delete -f "$DIR/manifests/schemas.yaml" --ignore-not-found
kubectl delete -f "$DIR/manifests/topics.yaml" --ignore-not-found
kubectl delete secret "$SECRET_KAFKA" "$SECRET_SR" "$SECRET_TABLEFLOW" -n "$NS" --ignore-not-found

if [[ "$UNINSTALL_CFK" == "true" ]]; then
  step "Confluent for Kubernetes"
  helm uninstall "$HELM_RELEASE" --namespace "$NS" 2>/dev/null \
    && info "operator '$HELM_RELEASE' removed" || info "operator '$HELM_RELEASE' not installed"
fi

if [[ "$DELETE_NAMESPACE" == "true" ]]; then
  kubectl delete namespace "$NS" --ignore-not-found
fi

# --- 4. Infrastructure --------------------------------------------------------------
if [[ "$DESTROY_INFRA" != "true" ]]; then
  step "Terraform"
  info "left intact. Re-run with DESTROY_INFRA=true to run 'terraform destroy'."
  info "Tableflow and the Kafka cluster keep billing until you do."
  exit 0
fi

step "Terraform destroy"
if ! terraform -chdir="$TF_DIR" destroy -auto-approve; then
  # The Azure provider integration commonly 409s while Tableflow cleanup is
  # still in flight. Dropping it from state lets the rest of the destroy finish;
  # deleting the environment removes the integration server-side anyway.
  info "destroy failed, retrying without the Azure provider integration"
  terraform -chdir="$TF_DIR" state rm confluent_provider_integration_setup.azure >/dev/null 2>&1
  terraform -chdir="$TF_DIR" state rm confluent_provider_integration_authorization.azure >/dev/null 2>&1
  terraform -chdir="$TF_DIR" destroy -auto-approve || info "destroy still failing; inspect $TF_DIR manually"
fi

rm -f "$DIR/demo.env" "$DIR"/manifests/*.yaml
info "done"
