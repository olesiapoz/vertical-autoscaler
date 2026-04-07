#/bin/bash
kubectl create namespace monitoring 
sleep 5 
kubectl apply -f sc-local.yaml 

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 
helm repo update 
sleep 5

current_path=$(pwd)

yq -i '.spec.hostPath.path = "'${current_path}'/monitoring/prometheus"' prometheus-pv-local.yaml
kubectl apply -f prometheus-pv-local.yaml 
kubectl apply -f prometheus-pvc-local.yaml 

sleep 5
# helm upgrade prometheus prometheus-community/prometheus --install --version 27.22.0 \
#     --reuse-values \
#     --namespace monitoring \
#     --set pushgateway.persistence.enabled=false \
#     --set alertmanager.persistence.enabled=false \
#     --set server.persistentVolume.volumeName="prometheus-pv-local" \
#     --set server.persistentVolume.existingClaim="prometheus-pvc-local" \
#     --set server.persistentVolume.storageClass="local-storage" \
#     --set server.persistentVolume.enabled=true &&

kubectl patch ds prometheus-prometheus-node-exporter -n monitoring \
  --type='json' \
  -p='[{"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/2/mountPropagation"}]'

yq -i '.spec.hostPath.path = "'${current_path}'/monitoring/grafana"' grafana-pv-local.yaml
kubectl apply -f grafana-pv-local.yaml 
kubectl apply -f grafana-pvc-local.yaml 
# helm repo add grafana https://grafana.github.io/helm-charts 
# helm upgrade  grafana grafana/grafana --version 9.2.9 --install \
#     --reuse-values \
#     --namespace monitoring \
#     --set persistence.enabled=true \
#     --set persistence.storageClassName="local-storage" \
#     --set persistence.existingClaim="grafana-pvc-local" 

helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm upgrade --install metrics-server metrics-server/metrics-server --version 3.13.0 \
    --namespace monitoring --set args={--kubelet-insecure-tls} --wait


# helm upgrade prometheus-adapter  prometheus-community/prometheus-adapter --namespace monitoring \
# --install --set prometheus.url=prometheus-server.monitoring.svc
# sleep 30

# helm install kube-state-metrics prometheus-community/kube-state-metrics \
   --namespace monitoring



#helm install prom-kube-stack oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack

helm upgrade --install prom-kube-stack prometheus-community/kube-prometheus-stack  \
    --version 78.5.0 \
    -f prom-stack-values.yaml \
    --namespace monitoring 

    sleep 100
sleep 100 && kubectl get all -n monitoring &&  kubectl get all -n kube-system


kubectl patch ds prom-kube-stack-prometheus-node-exporter -n monitoring \
  --type='json' \
  -p='[{"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/2/mountPropagation"}]'