## 2019.09.09 (1.12.10-gke.6, 1.13.10-gke.1, 1.14.6-gke.3, 1.15.3-gke.3)
* Added logic for surge upgrade protection for CA.
## 2019.05.22 (1.12.8-gke.7, 1.13.6-gke.6, 1.14.2-gke.3)
* Add functionality which delays node deletion in order to let other components prepare for deletion.
* Add autoprovisioning locations field which lets user to choose node locations for node pools created by NAP
* Add functionality creating GPU node pools even if GPUs are not available in all node locations.
* Pod sharding (no zone affinity optimization) (disabled on PROD)
## 2019.04.30 (1.11.10-gke.1, 1.12.8-gke.2, 1.13.6-gke.3, 1.14.2-gke.0)
* Add scopes and service accounts for NAP provisioned node pools from GKE API
* Add an updated GKE API v1beta1 client
## 2019.04.19 (1.11.9-gke.11; 1.12.7-gke.13; 1.13.5-gke.11; 1.14.1-gke.2)
* Add metrics for measuring SLO
* Limit number of API calls done to keep MIG instances list up to date in GCECache
* Update MIG target size cache via LIST api call
* CA Visibility flags stable; status, scale-up and scale-down events working
## 2019.01.07 (1.11.7-gke.0; 1.12.3-gke.10)
* Faster Stockout/Quota error handling
