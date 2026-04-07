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
helm upgrade --install prometheus prometheus-community/prometheus  \
    --version 27.22.0 \
    -f values-prometheus.yaml \
    --namespace monitoring \
    --set pushgateway.persistence.enabled=false \
    --set alertmanager.persistence.enabled=false \
    --set server.persistentVolume.volumeName="prometheus-pv-local" \
    --set server.persistentVolume.existingClaim="prometheus-pvc-local" \
    --set server.persistentVolume.storageClass="local-storage" \
    --set server.persistentVolume.enabled=true && 
    #--reuse-values \

kubectl patch ds prometheus-prometheus-node-exporter -n monitoring \
  --type='json' \
  -p='[{"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/2/mountPropagation"}]'

yq -i '.spec.hostPath.path = "'${current_path}'/monitoring/grafana"' grafana-pv-local.yaml
kubectl apply -f grafana-pv-local.yaml 
kubectl apply -f grafana-pvc-local.yaml 
helm repo add grafana https://grafana.github.io/helm-charts 
helm upgrade  grafana grafana/grafana --version 9.2.9 --install \
    --reuse-values \
    --namespace monitoring \
    --set persistence.enabled=true \
    --set persistence.storageClassName="local-storage" \
    --set persistence.existingClaim="grafana-pvc-local" 
sleep 100
kubectl get all -n monitoring


