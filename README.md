# Tableflow automation for Confluent Cloud

Automatically enable [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html)
on a Kafka topic the moment a schema is registered for it.

Tableflow materializes a Kafka topic as an Apache Iceberg table in your own
storage, and it refuses to work on schemaless topics. That makes "schema
registered" the natural trigger: you label a topic as *wanting* Tableflow, and a
small in-cluster watcher turns it on once the topic is actually eligible.

Three pieces, each owning one layer:

| Layer | Owns |
|---|---|
| `terraform/` | Confluent Cloud environment and cluster, Azure provider integration, ADLS Gen2 storage, service accounts, API keys |
| Confluent for Kubernetes (CFK) | Kafka topics and Avro schemas, declared as custom resources |
| `watcher/` | Turning Tableflow on when a labelled topic gains a schema |

The Terraform layer follows the same Azure provider-integration pattern as
[DebziumConfluentTableFlowIcebergSnowflakeNoSMT](https://github.com/MosheBlumbergX/DebziumConfluentTableFlowIcebergSnowflakeNoSMT),
including its hard-won operational workarounds.

---

## Contents

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
- [What you should see](#what-you-should-see)
- [Opting a topic in](#opting-a-topic-in)
- [Reading the logs](#reading-the-logs)
- [Running under CI/CD](#running-under-cicd)
- [Configuration reference](#configuration-reference)
- [Operational gotchas](#operational-gotchas)
- [Tear down](#tear-down)
- [Repository layout](#repository-layout)

---

## How it works

```
terraform/                          Kubernetes (CFK)
  environment, cluster              KafkaTopic CR            Schema CR
  provider integration              (label: tableflow        (subject:
  ADLS Gen2 + role assignments       .confluent.io/           <topic>-value)
  service accounts, API keys         enable=true)                 |
        |                                  |                     v
        |  outputs                         |          Confluent Cloud
        v                                  |          Schema Registry
   deploy.sh  ---- secrets ---->  tableflow-watcher <-- polls ---+
                                           |
                                           | POST /tableflow/v1/tableflow-topics
                                           v
                                  Iceberg tables in your ADLS Gen2 container
                                           |
                                           v
                                  Annotation written back:
                                  tableflow.confluent.io/status=Enabled
```

Every poll interval (30s by default) the watcher:

1. Lists `KafkaTopic` resources in the namespace carrying
   `tableflow.confluent.io/enable=true`.
2. Looks up `<topic>-value` in Schema Registry. Tableflow only supports
   `TopicNameStrategy`, so that is the subject it checks.
3. If the subject exists and Tableflow is not already on, calls the Tableflow
   API to enable it against the ADLS Gen2 container.
4. Annotates the `KafkaTopic` with `tableflow.confluent.io/status` so the
   decision is visible from `kubectl`.

The watcher is a single standard-library Python file mounted from a ConfigMap,
so there is no image to build and no dependencies to install.

> **The reconcile is one-way.** The watcher only ever turns Tableflow *on*.
> Removing the label does not turn it back off — see
> [Opting out does not disable Tableflow](#opting-out-does-not-disable-tableflow).

---

## Prerequisites

| Tool | Notes |
|---|---|
| Terraform ≥ 1.0 | provisions everything |
| Azure CLI | run `az login` first |
| `kubectl` | pointed at the cluster you want to deploy into |
| `helm` | installs the CFK operator |
| `envsubst` | renders the manifest templates (`gettext` package) |
| `python3` | runs the watcher; also used by the scripts |
| `confluent` CLI | optional, for verifying from the command line |
| `jq` | optional, for filtering the watcher's JSON logs |

You also need:

- A Kubernetes cluster in the current `kubectl` context.
- A Confluent Cloud API key with permission to create environments.
- An Azure subscription. **Tableflow is not available on Google Cloud**, and
  only some AWS and Azure regions support it — check the
  [supported regions](https://docs.confluent.io/cloud/current/clusters/regions.html).

> **This costs money.** A Standard Kafka cluster and every enabled Tableflow
> topic bill for as long as they exist. Run [Tear down](#tear-down) when done.

---

## Quickstart

### 1. Configure credentials

Terraform needs five values. Either put them in a `terraform.tfvars` file:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # then fill it in
```

…or, to keep credentials off disk, export them as environment variables:

```bash
export TF_VAR_prefix="moshe"
export TF_VAR_confluent_cloud_api_key="<key>"
export TF_VAR_confluent_cloud_api_secret="<secret>"
export TF_VAR_customer_azure_tenant_id="$(az account show --query tenantId -o tsv)"
export TF_VAR_azure_subscription_id="$(az account show --query id -o tsv)"
```

Environment variables only live as long as the shell. If you use them, keep the
same terminal open for the whole lifecycle, or you will have to re-export them
before `terraform destroy` can run.

### 2. Provision the infrastructure

```bash
terraform -chdir=terraform init
terraform -chdir=terraform apply
```

This creates **22 resources**: the Confluent Cloud environment and Standard
cluster on Azure, the Azure provider integration and its service principal, the
ADLS Gen2 account, container and role assignments, two service accounts, and
the Kafka, Schema Registry and Tableflow API keys.

The Tableflow API rejects ordinary Cloud API keys, which is why a separate key
with `managed_resource.id = "tableflow"` exists.

### 3. Deploy CFK and the watcher

```bash
./deploy.sh
```

`deploy.sh` reads `terraform output -json`, installs the CFK operator, writes
the three API keys as Kubernetes secrets, and applies the topics, schemas and
watcher. It never talks to Confluent Cloud directly — CFK and the watcher do
that. It is idempotent, so re-running it is safe.

If CFK is already installed in the cluster, skip the Helm step:

```bash
INSTALL_CFK=false ./deploy.sh
```

### 4. Verify

`deploy.sh` writes `demo.env` with the prefix and every derived name, so the
rest of the commands in this README can be copy-pasted:

```bash
source ./demo.env
kubectl get kafkatopic -n "$NS" \
  -o custom-columns=TOPIC:.metadata.name,TABLEFLOW:.metadata.annotations.tableflow\\.confluent\\.io/status
```

Confirm against Confluent Cloud:

```bash
confluent tableflow topic list --cluster "$CLUSTER_ID" --environment "$ENV_ID"
```

---

## What you should see

The example deploys three topics that exercise each path:

| Topic | Labelled | Schema | Expected outcome |
|---|---|---|---|
| `<prefix>-payments` | yes | yes | Tableflow enabled |
| `<prefix>-clickstream` | yes | no | stays `PendingSchema` |
| `<prefix>-audit-log` | no | yes | ignored |

### Watch the schema gate open

`<prefix>-clickstream` is deliberately left without a schema, which is what
parks it in `PendingSchema`. Register one and the watcher picks it up on its
next pass:

```bash
source ./demo.env
confluent schema-registry schema create \
  --subject "${PREFIX}-clickstream-value" --type avro \
  --schema <(echo '{"type":"record","name":"Click","fields":[{"name":"url","type":"string"}]}') \
  --environment "$ENV_ID"
```

Within one poll interval its status flips to `Enabled`.

---

## Opting a topic in

Add the label at topic creation time:

```yaml
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: moshe-payments
  namespace: moshe-confluent
  labels:
    tableflow.confluent.io/enable: "true"
```

Two optional annotations override the defaults per topic:

| Annotation | Default | Purpose |
|---|---|---|
| `tableflow.confluent.io/table-formats` | `ICEBERG` | Comma-separated table formats |
| `tableflow.confluent.io/retention-ms` | unset | Snapshot expiration in milliseconds |
| `tableflow.confluent.io/error-handling-mode` | `ERROR_HANDLING_MODE`, else Tableflow's default (`SUSPEND`) | What happens to a record that cannot be materialised: `SUSPEND` pauses, `SKIP` drops it, `LOG` sends it to a dead letter queue topic |
| `tableflow.confluent.io/error-handling-log-target` | `ERROR_HANDLING_LOG_TARGET`, else `error_log` | DLQ topic for `LOG` mode (created if missing). Ignored for other modes |

`LOG` only works for topics with Avro or Protobuf schemas, and the Tableflow API
key's service account needs write access to the DLQ topic or Tableflow pauses.
These settings are applied when Tableflow is first enabled for a topic. Changing
an annotation afterwards does not update an already enabled topic.

The watcher writes three more annotations back as status. Do not set these
yourself:

| Annotation | Meaning |
|---|---|
| `tableflow.confluent.io/status` | `PendingSchema`, `Enabled` or `Failed` |
| `tableflow.confluent.io/message` | Why it is in that state |
| `tableflow.confluent.io/last-transition-time` | When the status last changed |

### Topics created outside CFK

The watcher only sees `KafkaTopic` custom resources. A topic created directly in
Confluent Cloud — by a connector, say — is invisible to it even if a schema
exists. To bring one under management, write a `KafkaTopic` CR matching the
existing topic and CFK will adopt it. Be aware this transfers lifecycle
ownership: deleting the CR will then delete the topic.

---

## Reading the logs

The watcher emits one JSON object per line. Every record carries `ts`, `level`,
`msg` and the `component`, `namespace`, `environment` and `cluster` context, so
lines stay self-describing once they reach a log aggregator:

```bash
source ./demo.env
kubectl logs -f "deployment/$WATCHER_NAME" -n "$NS"
```

```json
{"ts":"2026-08-24T13:31:00.577Z","level":"INFO","msg":"tableflow-watcher starting","component":"tableflow-watcher","namespace":"moshe-confluent","environment":"env-1kjdz5","cluster":"lkc-mv228q2","selector":"tableflow.confluent.io/enable=true","storage":"AzureDataLakeStorageGen2","interval":30}
{"ts":"2026-08-24T13:31:01.375Z","level":"INFO","msg":"reconciling","component":"tableflow-watcher","namespace":"moshe-confluent","environment":"env-1kjdz5","cluster":"lkc-mv228q2","candidates":2,"enabled":0}
{"ts":"2026-08-24T13:31:01.376Z","level":"INFO","msg":"schema found, enabling Tableflow","component":"tableflow-watcher","namespace":"moshe-confluent","environment":"env-1kjdz5","cluster":"lkc-mv228q2","topic":"moshe-payments","subject":"moshe-payments-value","schema_version":1,"formats":"ICEBERG"}
{"ts":"2026-08-24T13:31:01.902Z","level":"INFO","msg":"Tableflow enabled","component":"tableflow-watcher","namespace":"moshe-confluent","environment":"env-1kjdz5","cluster":"lkc-mv228q2","topic":"moshe-payments"}
{"ts":"2026-08-24T13:31:01.903Z","level":"INFO","msg":"waiting for schema","component":"tableflow-watcher","namespace":"moshe-confluent","environment":"env-1kjdz5","cluster":"lkc-mv228q2","topic":"moshe-clickstream","subject":"moshe-clickstream-value"}
```

That is dense to read by eye, so filter it with `jq`:

```bash
# just the errors
kubectl logs "deployment/$WATCHER_NAME" -n "$NS" | jq -c 'select(.level == "ERROR")'

# a per-topic timeline
kubectl logs "deployment/$WATCHER_NAME" -n "$NS" \
  | jq -r 'select(.topic) | "\(.ts) \(.topic) \(.msg)"'
```

Set `LOG_FORMAT=text` for the plain single-line format when tailing by hand:

```bash
LOG_FORMAT=text ./deploy.sh
```

---

## Running under CI/CD

The label and the status annotations flow in opposite directions, which keeps a
GitOps pipeline and the watcher out of each other's way.

`tableflow.confluent.io/enable` is an **input your pipeline owns**. The watcher
only reads it, as a server-side label selector, and never writes labels. Flip it
by hand with `kubectl label` and the next sync reverts it, which is the point.

The status annotations are **outputs the watcher owns**. It writes them with a
JSON merge patch scoped to `metadata.annotations`, which registers it as its own
field manager alongside the others:

```bash
kubectl get kafkatopic "${PREFIX}-payments" -n "$NS" \
  -o jsonpath='{range .metadata.managedFields[*]}{.manager} ({.operation}){"\n"}{end}'
```

```
kubectl-client-side-apply (Update)   # your pipeline: labels and spec
manager (Update)                     # CFK operator: status and spec hash
Python-urllib (Update)               # the watcher: its annotations
```

`kubectl apply`, Argo CD's three-way diff and Flux's server-side apply all leave
the watcher's annotations alone, so none of them will show perpetual drift. A
sync strategy that replaces rather than patches — `kubectl replace`, or Argo CD
with `Replace=true` — does drop them, but only cosmetically: the watcher checks
live Confluent Cloud state *before* it reads annotations, so the next pass
restores them without re-calling the enable API. What you lose is provenance,
because the restored annotation reports the current phase rather than the schema
version that originally triggered the enable.

### Opting out does not disable Tableflow

Removing the label, or setting it to `false`, only takes the topic out of the
watcher's selector. **Tableflow stays running in Confluent Cloud, keeps writing
to ADLS Gen2 and keeps costing money.** Git says off, reality says on, and
nothing reports the difference.

Disabling is a separate, deliberate action, and it has to stop the watcher first
or the next pass re-enables the topic:

```bash
source ./demo.env
kubectl scale "deployment/$WATCHER_NAME" -n "$NS" --replicas=0
confluent tableflow topic disable "${PREFIX}-payments" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID"
# drop the label in git and sync, then bring the watcher back
kubectl scale "deployment/$WATCHER_NAME" -n "$NS" --replicas=1
```

`teardown.sh` follows the same order for the same reason.

Making opt-out declarative would mean listing every `KafkaTopic` and treating a
missing label as a disable, rather than filtering by selector at the API level.
That is a deliberate behaviour change, not an oversight: it makes a deleted line
in git drop a table someone may be querying.

### Do not blanket-ignore the annotation prefix

Inputs (`table-formats`, `retention-ms`) and outputs (`status`, `message`,
`last-transition-time`) share the `tableflow.confluent.io/` prefix. An Argo CD
`ignoreDifferences` rule on the whole prefix would mask your own declared inputs
along with the watcher's status, so ignore the three status keys individually.

---

## Configuration reference

### Naming

Everything is named `<prefix>-...`, where `prefix` defaults to `moshe`, so
several people can run this in a shared organization and subscription without
colliding. `deploy.sh` reads the prefix from the Terraform output, so setting it
in one place is enough.

| Resource | Default name |
|---|---|
| Confluent Cloud environment | `moshe-tableflow-demo` |
| Kafka cluster | `moshe-tableflow-cluster` |
| Azure resource group | `moshe-tableflow-rg` |
| ADLS Gen2 account | `moshetf<random>` |
| Kubernetes namespace | `moshe-confluent` |
| Watcher Deployment | `moshe-tableflow-watcher` |
| Topics | `moshe-payments`, `moshe-clickstream`, `moshe-audit-log` |

### Terraform variables

| Variable | Default | Purpose |
|---|---|---|
| `prefix` | `moshe` | Prepended to every resource name |
| `confluent_cloud_api_key` | — | Cloud API key allowed to create environments |
| `confluent_cloud_api_secret` | — | Its secret |
| `customer_azure_tenant_id` | — | Azure tenant for the provider integration |
| `azure_subscription_id` | — | Azure subscription to build storage in |
| `kafka_cluster_region` | `eastus2` | Must be a Tableflow-supported region |
| `kafka_cluster_availability` | `SINGLE_ZONE` | Cluster availability |
| `storage_account_name` | generated | `<prefix>tf<random>`; see the gotcha below |
| `storage_container_name` | `tableflow-data` | Container holding the Iceberg tables |

### `deploy.sh`

| Variable | Default | Purpose |
|---|---|---|
| `INSTALL_CFK` | `true` | Set `false` if the operator is already installed |
| `LOG_FORMAT` | `json` | `text` for human-readable watcher logs |
| `POLL_INTERVAL_SECONDS` | `30` | How often the watcher reconciles |
| `WATCHER_IMAGE` | `python:3.13-alpine` | Any image with a `python3` |
| `NS` | `<prefix>-confluent` | Target namespace |
| `HELM_RELEASE` | `<prefix>-operator` | CFK Helm release name |

### `teardown.sh`

| Variable | Default | Purpose |
|---|---|---|
| `DESTROY_INFRA` | `false` | `true` also runs `terraform destroy` |
| `UNINSTALL_CFK` | `true` | Remove the CFK operator |
| `DELETE_NAMESPACE` | `false` | `true` also deletes the namespace |
| `PREFIX` | `moshe` | Used to derive names if `demo.env` is missing |

---

## Operational gotchas

### Recovering a suspended Tableflow topic

Any schema change Tableflow rejects — a required field added, a field deleted —
leaves the topic suspended, and it does **not** resume on its own once the
schema is corrected. Check it with:

```bash
source ./demo.env
TF_KEY=$(terraform -chdir=terraform output -raw tableflow_api_key)
TF_SECRET=$(terraform -chdir=terraform output -raw tableflow_api_secret)

curl -s -u "$TF_KEY:$TF_SECRET" \
  "https://api.confluent.cloud/tableflow/v1/tableflow-topics/${PREFIX}-payments?environment=$ENV_ID&spec.kafka_cluster=$CLUSTER_ID" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['status']['phase'], d['status'].get('error_message'))"
```

Register a corrected schema version first, then clear the suspension. Note that
`environment` goes inside `spec`, unlike the query string:

```bash
curl -s -X PATCH -u "$TF_KEY:$TF_SECRET" \
  "https://api.confluent.cloud/tableflow/v1/tableflow-topics/${PREFIX}-payments?environment=$ENV_ID&spec.kafka_cluster=$CLUSTER_ID" \
  -H 'Content-Type: application/json' \
  -d "{\"spec\":{\"environment\":{\"id\":\"$ENV_ID\"},\"kafka_cluster\":{\"id\":\"$CLUSTER_ID\"},\"suspended\":false}}"
```

### Never reuse a storage account name

`storage_account_name` has no default; the configuration generates
`<prefix>tf<random>` instead, and the random suffix regenerates after every
destroy. Reusing a name shortly after destroying it produces a stack where
Tableflow writes correctly but readers cannot fetch the parquet files:

```
100483 (22000): Unable to download parquet file: .../data/....parquet.
```

That error is misleading — the file is present and every permission checks out.
Rebuilding with a fresh name is the only reliable fix. For the same reason, do
not try to swap the storage account on a live Tableflow topic; Confluent
enforces a grace period of up to an hour before the old association clears.

---

## Tear down

Tableflow and the Kafka cluster bill for as long as they exist.

```bash
./teardown.sh                      # Kubernetes resources only
DESTROY_INFRA=true ./teardown.sh   # also run terraform destroy
```

`teardown.sh` deletes the watcher Deployment first, then disables every
Tableflow topic through the API before touching Terraform. That ordering
matters: the watcher would otherwise re-enable the topics, and the Azure
provider integration cannot be destroyed while a Tableflow topic still
references it. If `terraform destroy` still fails with `409 Conflict` on the
provider integration, the script drops it from state and retries. Deleting the
environment removes the integration server-side, so nothing is orphaned.

If you supplied credentials as `TF_VAR_*` environment variables rather than a
`terraform.tfvars` file, re-export them before running with `DESTROY_INFRA=true`.

---

## Repository layout

```
.
├── deploy.sh                       Installs CFK, writes secrets, applies manifests
├── teardown.sh                     Disables Tableflow, removes resources, optional destroy
├── manifests/
│   ├── topics.yaml.template        Three KafkaTopic CRs covering each path
│   ├── schemas.yaml.template       Avro Schema CRs and their ConfigMaps
│   └── tableflow-watcher.yaml.template   RBAC, ConfigMap and Deployment
├── terraform/
│   ├── main.tf                     Confluent Cloud, Azure integration, ADLS Gen2, keys
│   ├── variables.tf                Inputs, with validation on prefix and account name
│   ├── outputs.tf                  Flat outputs consumed by deploy.sh
│   └── terraform.tfvars.example    Copy to terraform.tfvars and fill in
└── watcher/
    └── tableflow_watcher.py        The reconcile loop, standard library only
```

Generated at runtime and git-ignored: `demo.env`, the rendered `manifests/*.yaml`,
`terraform.tfvars`, and Terraform state. These contain credentials or
environment-specific identifiers — do not commit them.
