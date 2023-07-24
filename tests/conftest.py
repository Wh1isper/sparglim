#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import pytest


@pytest.fixture
def cluster_ca_file(tmpdir) -> str:
    cluster_ca_file = tmpdir.join("cluster_ca_file")
    cluster_ca_file.write(
        """
-----BEGIN CERTIFICATE-----
NotARealCertificate==
-----END CERTIFICATE-----
        """
    )
    yield str(cluster_ca_file)
    cluster_ca_file.remove()


@pytest.fixture
def client_ca_file(tmpdir) -> str:
    client_ca_file = tmpdir.join("client_ca_file")
    client_ca_file.write(
        """
-----BEGIN CERTIFICATE-----
NotARealCertificate==
-----END CERTIFICATE-----
        """
    )
    yield str(client_ca_file)
    client_ca_file.remove()


@pytest.fixture
def client_key_file(tmpdir) -> str:
    client_key_file = tmpdir.join("client_key_file")
    client_key_file.write(
        """
-----BEGIN CERTIFICATE-----
NotARealCertificate==
-----END CERTIFICATE-----
        """
    )
    yield str(client_key_file)
    client_key_file.remove()


@pytest.fixture
def k8s_config_path(tmpdir, client_key_file, client_ca_file, cluster_ca_file):
    k8s_config_path = tmpdir.join("k8s_config")
    k8s_config_path.write(
        f"""
apiVersion: v1
clusters:
- cluster:
    certificate-authority: {cluster_ca_file}
    extensions:
    - extension:
        last-update: Sun, 16 Jul 2023 15:22:10 CST
        provider: minikube.sigs.k8s.io
        version: v1.28.0
      name: cluster_info
    server: https://172.27.211.155:8443
  name: minikube
contexts:
- context:
    cluster: minikube
    user: minikube
  name: minikube
current-context: minikube
kind: Config
users:
- name: minikube
  user:
    client-certificate: {client_ca_file}
    client-key: {client_key_file}
"""
    )
    yield k8s_config_path
    k8s_config_path.remove()
