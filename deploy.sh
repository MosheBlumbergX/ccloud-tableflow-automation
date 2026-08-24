#!/usr/bin/env bash
# Deploy the Kubernetes half of the example on top of the infrastructure that
# terraform/ created: install Confluent for Kubernetes, turn the Terraform
# outputs into secrets, and apply the topics, schemas and Tableflow watcher.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$DIR/terraform"

WATCHER_IMAGE="${WATCHER_IMAGE:-python:3.13-alpine}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-30}"
INSTALL_CFK="${INSTALL_CFK:-true}"
LOG_FORMAT="${LOG_FORMAT:-json}"

step() { printf '\n\033[1m=== %s ===\033[0m\n' "$1"; }
info() { printf '  %s\n' "$1"; }
die()  { printf '\033[31mERROR: %s\033[0m\n' "$1" >&2; exit 1; }

require() { command -v "$1" >/dev/null 2>&1 || die "$1 is required but not on PATH"; }

step "Preflight"
for tool in terraform kubectl helm envsubst python3; do require "$tool"; done
kubectl cluster-info >/dev/null 2>&1 || die "no reachable Kubernetes cluster in the current kubectl context"
[[ -d "$TF_DIR/.terraform" ]] || die "run 'terraform init && terraform apply' in $TF_DIR first"
info "kubectl context: $(kubectl config current-context)"

# --- 1. Read the Terraform outputs ------------------------------------------------
step "Reading Terraform outputs"
TF_JSON="$(terraform -chdir="$TF_DIR" output -json 2>/dev/null)" \
  || die "could not read Terraform outputs. Has 'terraform apply' completed in $TF_DIR?"

read_output() {
  printf '%s' "$TF_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
key = '$1'
if key not in data:
    sys.exit(f'missing Terraform output: {key}')
print(data[key]['value'])
"
}

PREFIX="$(read_output prefix)"
ENV_ID="$(read_output environment_id)"
CLUSTER_ID="$(read_output kafka_cluster_id)"
KAFKA_REST="$(read_output kafka_rest_endpoint)"
SR_ENDPOINT="$(read_output schema_registry_endpoint)"
PROVIDER_INTEGRATION_ID="$(read_output provider_integration_id)"
STORAGE_ACCOUNT_NAME="$(read_output storage_account_name)"
STORAGE_CONTAINER_NAME="$(read_output storage_container_name)"

KAFKA_KEY="$(read_output kafka_api_key)"
KAFKA_SECRET="$(read_output kafka_api_secret)"
SR_KEY="$(read_output schema_registry_api_key)"
SR_SECRET="$(read_output schema_registry_api_secret)"
TF_KEY="$(read_output tableflow_api_key)"
TF_SECRET="$(read_output tableflow_api_secret)"

# Kubernetes object names, all derived from the same prefix Terraform used.
NS="${NS:-${PREFIX}-confluent}"
HELM_RELEASE="${HELM_RELEASE:-${PREFIX}-operator}"
SECRET_KAFKA="${PREFIX}-cloud-rest-access"
SECRET_SR="${PREFIX}-cloud-sr-access"
SECRET_TABLEFLOW="${PREFIX}-tableflow-api-access"
WATCHER_NAME="${PREFIX}-tableflow-watcher"
SCRIPT_CONFIGMAP="${PREFIX}-tableflow-watcher-script"

info "prefix:      $PREFIX"
info "environment: $ENV_ID"
info "cluster:     $CLUSTER_ID"
info "storage:     $STORAGE_ACCOUNT_NAME/$STORAGE_CONTAINER_NAME via $PROVIDER_INTEGRATION_ID"
info "namespace:   $NS"

# --- 2. Confluent for Kubernetes ----------------------------------------------------
step "Confluent for Kubernetes"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
if [[ "$INSTALL_CFK" == "true" ]]; then
  helm repo add confluentinc https://packages.confluent.io/helm >/dev/null 2>&1 || true
  helm repo update confluentinc >/dev/null
  helm upgrade --install "$HELM_RELEASE" confluentinc/confluent-for-kubernetes \
    --namespace "$NS" --wait --timeout 5m >/dev/null
  info "operator '$HELM_RELEASE' installed in namespace $NS"
else
  info "skipping operator install (INSTALL_CFK=false)"
fi

# --- 3. Secrets ----------------------------------------------------------------------
step "Kubernetes secrets"
put_secret() {
  local name="$1" key="$2" secret="$3"
  kubectl create secret generic "$name" \
    --from-file=basic.txt=<(printf 'username=%s\npassword=%s\n' "$key" "$secret") \
    --namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  info "secret/$name"
}
put_secret "$SECRET_KAFKA"     "$KAFKA_KEY" "$KAFKA_SECRET"
put_secret "$SECRET_SR"        "$SR_KEY"    "$SR_SECRET"
put_secret "$SECRET_TABLEFLOW" "$TF_KEY"    "$TF_SECRET"

# --- 4. Render and apply manifests ------------------------------------------------------
step "Deploying topics, schemas and the watcher"
SCRIPT_CHECKSUM="$(python3 -c "
import hashlib
print(hashlib.sha256(open('$DIR/watcher/tableflow_watcher.py','rb').read()).hexdigest()[:16])
")"

export PREFIX NS CC_ENVIRONMENT_ID="$ENV_ID" CC_CLUSTER_ID="$CLUSTER_ID"
export KAFKA_REST SR_ENDPOINT WATCHER_IMAGE POLL_INTERVAL_SECONDS SCRIPT_CHECKSUM LOG_FORMAT
export SECRET_KAFKA SECRET_SR SECRET_TABLEFLOW WATCHER_NAME SCRIPT_CONFIGMAP
export PROVIDER_INTEGRATION_ID STORAGE_ACCOUNT_NAME STORAGE_CONTAINER_NAME

for template in topics schemas tableflow-watcher; do
  envsubst <"$DIR/manifests/${template}.yaml.template" >"$DIR/manifests/${template}.yaml"
done

kubectl create configmap "$SCRIPT_CONFIGMAP" \
  --from-file=tableflow_watcher.py="$DIR/watcher/tableflow_watcher.py" \
  --namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl apply -f "$DIR/manifests/topics.yaml"
kubectl apply -f "$DIR/manifests/schemas.yaml"
kubectl apply -f "$DIR/manifests/tableflow-watcher.yaml"
kubectl rollout status "deployment/$WATCHER_NAME" --namespace "$NS" --timeout=180s

# --- 5. Summary ---------------------------------------------------------------------------
step "Summary"
cat >"$DIR/demo.env" <<EOF
PREFIX=${PREFIX}
ENV_ID=${ENV_ID}
CLUSTER_ID=${CLUSTER_ID}
KAFKA_REST=${KAFKA_REST}
SR_ENDPOINT=${SR_ENDPOINT}
PROVIDER_INTEGRATION_ID=${PROVIDER_INTEGRATION_ID}
STORAGE_ACCOUNT_NAME=${STORAGE_ACCOUNT_NAME}
STORAGE_CONTAINER_NAME=${STORAGE_CONTAINER_NAME}
NS=${NS}
HELM_RELEASE=${HELM_RELEASE}
SECRET_KAFKA=${SECRET_KAFKA}
SECRET_SR=${SECRET_SR}
SECRET_TABLEFLOW=${SECRET_TABLEFLOW}
WATCHER_NAME=${WATCHER_NAME}
SCRIPT_CONFIGMAP=${SCRIPT_CONFIGMAP}
EOF
info "wrote $DIR/demo.env"

kubectl get kafkatopic,schema -n "$NS"
echo
info "follow the watcher with:"
info "  kubectl logs -f deployment/$WATCHER_NAME -n $NS"
info "check what it decided with:"
info "  kubectl get kafkatopic -n $NS -o custom-columns=TOPIC:.metadata.name,TABLEFLOW:.metadata.annotations.tableflow\\.confluent\\.io/status"
