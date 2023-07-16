#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os

from kubernetes import config
from kubernetes.client import Configuration

INCLUSTER = os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace")

if INCLUSTER:
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
        default_namespace = f.read()
else:
    default_namespace = "spark-app"

NAMESPACE = os.getenv("SPARK_EXECUTOR_NS", default_namespace)


def get_k8s_config(config_file_path=None):
    if INCLUSTER:
        config.load_incluster_config()
    else:
        config_file_path = config_file_path or os.path.expanduser("~/.kube/config")
        config.load_kube_config(config_file_path)
    k8s_config = Configuration.get_default_copy()
    token = k8s_config.api_key.get("authorization")
    host = k8s_config.host
    ca = k8s_config.ssl_ca_cert
    key_file = k8s_config.key_file
    cert_file = k8s_config.cert_file

    return host, token, ca, key_file, cert_file


def get_namespace():
    if not INCLUSTER:
        return "sparglim"
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
        default_namespace = f.read()
    return default_namespace
