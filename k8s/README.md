# linkding on k3s

Self-hosted bookmark manager running on a single-node k3s cluster at home. No cloud, no CI, everything applied with kubectl.

I built this to get comfortable with the basic Kubernetes objects instead of just reading about them.

## What's inside

| File | What it does |
|---|---|
| `k8s/namespace.yaml` | keeps the app out of `default` |
| `k8s/pvc.yaml` | 2Gi volume so bookmarks survive a pod restart |
| `k8s/secret.yaml` | admin user and password |
| `k8s/deployment.yaml` | the app, with probes and resource limits |
| `k8s/service.yaml` | stable ClusterIP in front of the pod |
| `k8s/ingress.yaml` | traefik route for `linkding.local` |

## Requirements

- k3s (traefik and local-path are enabled by default)
- kubectl

k3s install:

```bash
curl -sfL https://get.k3s.io | sh -
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER ~/.kube/config
```

## Deploy

Change the password in `k8s/secret.yaml` first.

```bash
kubectl apply -f k8s/
kubectl -n linkding get pods
```

Point the hostname at the node:

```bash
echo "127.0.0.1 linkding.local" | sudo tee -a /etc/hosts
```

Then open http://linkding.local and log in with the credentials from the secret.

## Notes

**Fedora firewall.** On Fedora the pod got a Bad Gateway from traefik even though the pod, the service and the endpoints all looked fine. firewalld was dropping traffic between pods. Fix:

```bash
sudo firewall-cmd --permanent --zone=trusted --add-source=10.42.0.0/16
sudo firewall-cmd --permanent --zone=trusted --add-source=10.43.0.0/16
sudo firewall-cmd --reload
```

`10.42.0.0/16` is the pod network, `10.43.0.0/16` is the service network.

**Strategy is Recreate, not RollingUpdate.** The volume is ReadWriteOnce, so two pods can't hold it at the same time. A rolling update would deadlock waiting for the old pod to release it.

**The image tag is pinned.** `latest` means a restart can silently pull a different version.

## Checking persistence

```bash
kubectl -n linkding delete pod -l app=linkding
```

The pod comes back and the bookmarks are still there.

## Troubleshooting

```bash
kubectl -n linkding get pods
kubectl -n linkding logs deploy/linkding
kubectl -n linkding describe pod -l app=linkding
kubectl -n linkding get endpoints linkding
```

Empty endpoints usually means the service selector doesn't match the pod labels.

## Removing it

```bash
kubectl delete -f k8s/
```

The PVC goes away with the namespace, so this deletes the bookmarks too.

## Next

- TLS with cert-manager and a local CA
- Postgres instead of the bundled SQLite
- NetworkPolicy (needs a CNI that supports it, flannel in k3s doesn't)
