#!/usr/bin/env python3
"""Enable Tableflow on Confluent Cloud topics once their schema is registered.

Watches CFK KafkaTopic custom resources carrying an opt-in label, waits for the
topic's TopicNameStrategy value subject to appear in Schema Registry, then calls
the Confluent Cloud Tableflow API to materialize the topic as an Iceberg table.
Progress is written back onto the KafkaTopic resource as annotations.

Only the Python standard library is used so the container image needs no build
step or package installs.
"""

from __future__ import annotations

import base64
import json
import os
import signal
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
GROUP = "platform.confluent.io"
VERSION = "v1beta1"

ANN_PREFIX = "tableflow.confluent.io"
ANN_STATUS = f"{ANN_PREFIX}/status"
ANN_MESSAGE = f"{ANN_PREFIX}/message"
ANN_UPDATED = f"{ANN_PREFIX}/last-transition-time"
ANN_TABLE_FORMATS = f"{ANN_PREFIX}/table-formats"
ANN_RETENTION_MS = f"{ANN_PREFIX}/retention-ms"

STATUS_PENDING = "PendingSchema"
STATUS_ENABLED = "Enabled"
STATUS_FAILED = "Failed"


# One JSON object per line, which is what log shippers expect. Set
# LOG_FORMAT=text for a human-readable stream when running this by hand.
LOG_FORMAT = os.environ.get("LOG_FORMAT", "json").strip().lower()

# Keys owned by the log envelope. A caller field of the same name is prefixed
# rather than silently overwriting the envelope.
RESERVED_LOG_KEYS = frozenset({"ts", "level", "msg", "component"})

# Merged into every record so aggregated lines identify themselves without
# needing to be correlated back to a pod.
_log_context: dict[str, Any] = {"component": "tableflow-watcher"}


def set_log_context(**fields: Any) -> None:
    _log_context.update(fields)


class ConfigError(Exception):
    pass


def log(level: str, msg: str, /, **fields: Any) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    if LOG_FORMAT == "text":
        extra = " ".join(f"{key}={value}" for key, value in fields.items())
        print(f"{stamp} {level:<5} {msg} {extra}".rstrip(), flush=True)
        return

    record: dict[str, Any] = {"ts": stamp, "level": level, "msg": msg}
    record.update(_log_context)
    for key, value in fields.items():
        record[f"field_{key}" if key in RESERVED_LOG_KEYS else key] = value

    # default=str keeps an unexpected value from taking the process down.
    print(json.dumps(record, default=str), flush=True)


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default if default is not None else "")
    if required and not value:
        raise ConfigError(f"environment variable {name} is required")
    return value


def read_credentials(path: str) -> tuple[str, str]:
    """Parse a CFK-style `username=`/`password=` credentials file."""
    values: dict[str, str] = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    try:
        return values["username"], values["password"]
    except KeyError as exc:
        raise ConfigError(f"{path} is missing a {exc} entry") from exc


def basic_auth(key: str, secret: str) -> str:
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return f"Basic {token}"


def request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: Any = None,
    context: ssl.SSLContext | None = None,
    timeout: int = 30,
) -> tuple[int, Any]:
    """Perform an HTTP request, returning the status code and decoded JSON body.

    HTTP error responses are returned rather than raised so that callers can
    treat 404 and 409 as ordinary control flow.
    """
    data = None
    headers = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode()
        headers.setdefault("Content-Type", "application/json")
    headers.setdefault("Accept", "application/json")

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return exc.code, {"raw": raw.decode(errors="replace")}


def error_detail(payload: Any) -> str:
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            return first.get("detail") or first.get("title") or json.dumps(first)
        if "message" in payload:
            return str(payload["message"])
        return json.dumps(payload)[:300]
    return str(payload)[:300]


class KubernetesClient:
    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        host = env("KUBERNETES_SERVICE_HOST", required=True)
        port = env("KUBERNETES_SERVICE_PORT", "443")
        self.base = f"https://{host}:{port}"
        with open(f"{SA_DIR}/token", encoding="utf-8") as handle:
            self.token = handle.read().strip()
        self.context = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")

    def _headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.token}"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def list_kafka_topics(self, label_selector: str) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"labelSelector": label_selector})
        url = (
            f"{self.base}/apis/{GROUP}/{VERSION}"
            f"/namespaces/{self.namespace}/kafkatopics?{query}"
        )
        status, payload = request(url, headers=self._headers(), context=self.context)
        if status != 200:
            raise RuntimeError(f"listing KafkaTopics failed ({status}): {error_detail(payload)}")
        if not isinstance(payload, dict):
            return []
        return payload.get("items") or []

    def annotate(self, name: str, annotations: dict[str, str]) -> None:
        url = (
            f"{self.base}/apis/{GROUP}/{VERSION}"
            f"/namespaces/{self.namespace}/kafkatopics/{urllib.parse.quote(name)}"
        )
        patch = {"metadata": {"annotations": annotations}}
        status, payload = request(
            url,
            method="PATCH",
            headers=self._headers("application/merge-patch+json"),
            body=patch,
            context=self.context,
        )
        if status not in (200, 201):
            log("WARN", "could not annotate KafkaTopic", topic=name, status=status,
                detail=error_detail(payload))


class SchemaRegistryClient:
    def __init__(self, endpoint: str, key: str, secret: str) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.auth = basic_auth(key, secret)

    def latest_version(self, subject: str) -> dict[str, Any] | None:
        """Return the latest registered schema for a subject, or None if absent."""
        url = f"{self.endpoint}/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest"
        status, payload = request(url, headers={"Authorization": self.auth})
        if status == 200:
            return payload
        # 404 with error code 40401 (subject) or 40402 (version) means "not yet registered".
        if status == 404:
            return None
        raise RuntimeError(
            f"Schema Registry lookup for {subject} failed ({status}): {error_detail(payload)}"
        )


def build_storage_spec() -> dict[str, str]:
    """Build the Tableflow storage block from the environment.

    Terraform provisions an ADLS Gen2 account and the provider integration that
    lets Tableflow reach it, so that is the default. Confluent Managed Storage
    stays available for a quick run without any cloud storage of your own.
    """
    kind = env("TABLEFLOW_STORAGE_KIND", "AzureDataLakeStorageGen2")

    if kind == "Managed":
        return {"kind": "Managed"}

    if kind == "AzureDataLakeStorageGen2":
        return {
            "kind": kind,
            "storage_account_name": env("AZURE_STORAGE_ACCOUNT_NAME", required=True),
            "container_name": env("AZURE_CONTAINER_NAME", required=True),
            "provider_integration_id": env("PROVIDER_INTEGRATION_ID", required=True),
        }

    if kind == "ByobAws":
        return {
            "kind": kind,
            "bucket_name": env("AWS_BUCKET_NAME", required=True),
            "provider_integration_id": env("PROVIDER_INTEGRATION_ID", required=True),
        }

    raise ConfigError(f"unsupported TABLEFLOW_STORAGE_KIND {kind!r}")


class TableflowClient:
    BASE = "https://api.confluent.cloud/tableflow/v1/tableflow-topics"

    def __init__(
        self,
        key: str,
        secret: str,
        environment_id: str,
        cluster_id: str,
        storage: dict[str, str],
    ) -> None:
        self.auth = basic_auth(key, secret)
        self.environment_id = environment_id
        self.cluster_id = cluster_id
        self.storage = storage

    def _scope(self) -> dict[str, str]:
        return {"environment": self.environment_id, "spec.kafka_cluster": self.cluster_id}

    def list_enabled(self) -> dict[str, dict[str, Any]]:
        """Return every Tableflow-enabled topic on the cluster, keyed by topic name."""
        enabled: dict[str, dict[str, Any]] = {}
        query = dict(self._scope(), page_size="100")
        url = f"{self.BASE}?{urllib.parse.urlencode(query)}"
        while url:
            status, payload = request(url, headers={"Authorization": self.auth})
            if status != 200:
                raise RuntimeError(
                    f"listing Tableflow topics failed ({status}): {error_detail(payload)}"
                )
            # The API sends "data": null rather than [] when nothing is enabled,
            # and an empty string for the last page's "next" link.
            for item in payload.get("data") or []:
                name = (item.get("spec") or {}).get("display_name")
                if name:
                    enabled[name] = item
            url = (payload.get("metadata") or {}).get("next") or None
        return enabled

    def enable(self, topic: str, table_formats: list[str], retention_ms: str | None) -> dict[str, Any]:
        spec: dict[str, Any] = {
            "display_name": topic,
            "storage": self.storage,
            "table_formats": table_formats,
            "environment": {"id": self.environment_id},
            "kafka_cluster": {"id": self.cluster_id},
        }
        if retention_ms:
            spec["config"] = {"retention_ms": retention_ms}

        status, payload = request(
            self.BASE, method="POST", headers={"Authorization": self.auth}, body={"spec": spec}
        )
        if status in (200, 201, 202):
            return payload or {}
        if status == 409:
            log("INFO", "Tableflow already enabled", topic=topic)
            return {}
        raise RuntimeError(f"enabling Tableflow failed ({status}): {error_detail(payload)}")


def topic_name(resource: dict[str, Any]) -> str:
    """CFK lets spec.name override the resource name as the Kafka topic name."""
    spec = resource.get("spec") or {}
    return spec.get("name") or resource["metadata"]["name"]


def table_formats_for(resource: dict[str, Any], default: list[str]) -> list[str]:
    annotations = resource["metadata"].get("annotations") or {}
    raw = annotations.get(ANN_TABLE_FORMATS)
    if not raw:
        return default
    formats = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return formats or default


class Reconciler:
    def __init__(
        self,
        kube: KubernetesClient,
        registry: SchemaRegistryClient,
        tableflow: TableflowClient,
        label_selector: str,
        default_formats: list[str],
    ) -> None:
        self.kube = kube
        self.registry = registry
        self.tableflow = tableflow
        self.label_selector = label_selector
        self.default_formats = default_formats

    def _record(self, resource_name: str, status: str, message: str) -> None:
        self.kube.annotate(
            resource_name,
            {
                ANN_STATUS: status,
                ANN_MESSAGE: message[:250],
                ANN_UPDATED: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )

    def reconcile_once(self) -> None:
        topics = self.kube.list_kafka_topics(self.label_selector)
        if not topics:
            log("INFO", "no labelled KafkaTopics found", selector=self.label_selector)
            return

        already_enabled = self.tableflow.list_enabled()
        log("INFO", "reconciling", candidates=len(topics), enabled=len(already_enabled))

        for resource in topics:
            resource_name = resource["metadata"]["name"]
            topic = topic_name(resource)
            try:
                self._reconcile_topic(resource, resource_name, topic, already_enabled)
            except Exception as exc:  # keep one bad topic from stalling the rest
                log(
                    "ERROR",
                    "reconcile failed",
                    topic=topic,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                self._record(resource_name, STATUS_FAILED, str(exc))

    def _reconcile_topic(
        self,
        resource: dict[str, Any],
        resource_name: str,
        topic: str,
        already_enabled: dict[str, dict[str, Any]],
    ) -> None:
        annotations = resource["metadata"].get("annotations") or {}

        if topic in already_enabled:
            phase = already_enabled[topic].get("status", {}).get("phase", "UNKNOWN")
            if annotations.get(ANN_STATUS) != STATUS_ENABLED:
                self._record(resource_name, STATUS_ENABLED, f"Tableflow phase {phase}")
            log("INFO", "already enabled", topic=topic, phase=phase)
            return

        subject = f"{topic}-value"
        schema = self.registry.latest_version(subject)
        if schema is None:
            log("INFO", "waiting for schema", topic=topic, subject=subject)
            if annotations.get(ANN_STATUS) != STATUS_PENDING:
                self._record(resource_name, STATUS_PENDING, f"waiting for subject {subject}")
            return

        formats = table_formats_for(resource, self.default_formats)
        retention = annotations.get(ANN_RETENTION_MS)
        log(
            "INFO",
            "schema found, enabling Tableflow",
            topic=topic,
            subject=subject,
            schema_version=schema.get("version"),
            formats=",".join(formats),
        )
        self.tableflow.enable(topic, formats, retention)
        self._record(
            resource_name,
            STATUS_ENABLED,
            f"enabled from {subject} v{schema.get('version')}",
        )
        log("INFO", "Tableflow enabled", topic=topic)


def build_reconciler() -> tuple[Reconciler, int]:
    namespace = env("WATCH_NAMESPACE", required=True)
    label_selector = env("LABEL_SELECTOR", "tableflow.confluent.io/enable=true")
    interval = int(env("POLL_INTERVAL_SECONDS", "30"))
    default_formats = [
        item.strip().upper() for item in env("DEFAULT_TABLE_FORMATS", "ICEBERG").split(",") if item.strip()
    ]

    sr_key, sr_secret = read_credentials(env("SR_CREDENTIALS_FILE", "/mnt/secrets/schema-registry/basic.txt"))
    tf_key, tf_secret = read_credentials(env("TABLEFLOW_CREDENTIALS_FILE", "/mnt/secrets/tableflow/basic.txt"))

    storage = build_storage_spec()

    kube = KubernetesClient(namespace)
    registry = SchemaRegistryClient(env("SR_ENDPOINT", required=True), sr_key, sr_secret)
    tableflow = TableflowClient(
        tf_key,
        tf_secret,
        env("CC_ENVIRONMENT_ID", required=True),
        env("CC_CLUSTER_ID", required=True),
        storage,
    )

    # Carried on every subsequent line so aggregated logs stay self-describing.
    set_log_context(
        namespace=namespace,
        environment=tableflow.environment_id,
        cluster=tableflow.cluster_id,
    )

    log(
        "INFO",
        "tableflow-watcher starting",
        selector=label_selector,
        storage=storage["kind"],
        interval=interval,
    )
    return Reconciler(kube, registry, tableflow, label_selector, default_formats), interval


def main() -> int:
    try:
        reconciler, interval = build_reconciler()
    except ConfigError as exc:
        log("ERROR", "invalid configuration", error=str(exc))
        return 2

    running = True

    def stop(signum: int, _frame: Any) -> None:
        nonlocal running
        log("INFO", "shutting down", signal=signum)
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while running:
        try:
            reconciler.reconcile_once()
        except Exception as exc:
            log("ERROR", "reconcile loop error", error=str(exc), error_type=type(exc).__name__)
        for _ in range(interval):
            if not running:
                break
            time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
