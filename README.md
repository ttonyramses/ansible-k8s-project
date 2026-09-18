# Cluster Kubernetes avec Ansible et Vagrant

Ce projet permet de déployer automatiquement un cluster Kubernetes hautement disponible avec 3 masters et 3 workers, ainsi que plusieurs services additionnels comme Kafka, Zookeeper et Longhorn.

## Prérequis

- VirtualBox
- Vagrant
- Ansible (version 2.12+)
- Accès à Internet pour les machines virtuelles

## Structure du projet

```
.
├── ansible.cfg                # Configuration Ansible
├── inventory.ini              # Inventaire des hôtes
├── group_vars                 # Variables pour les groupes
│   ├── all.yml                # Variables globales
│   ├── masters.yml            # Variables pour les masters
│   └── workers.yml            # Variables pour les workers
├── vagrant                    
│   └── Vagrantfile            # Votre fichier Vagrant
├── playbooks                  
│   ├── main.yml               # Playbook principal
│   ├── prerequisites.yml      # Installation des prérequis
│   ├── kubernetes.yml         # Installation Kubernetes
│   ├── keepalived.yml         # VIP haute disponibilité du plan de contrôle
│   ├── master_init.yml        # Initialisation du master principal
│   ├── masters_join.yml       # Ajout des autres masters
│   ├── etcd_backup.yml        # Sauvegardes automatiques d'etcd
│   ├── workers_join.yml       # Ajout des workers
│   ├── network.yml            # Configuration du réseau Calico
│   ├── metrics.yml            # Installation des metrics
│   ├── loadbalancer.yml       # Installation de metallb
│   ├── longhorn.yml           # Installation de Longhorn
│   ├── kafka.yml              # Installation de Kafka et Zookeeper
│   └── test.yml               # Tests de validation
└── roles                      # Rôles Ansible (structure minimale)
```

## Comment utiliser

1. Démarrer les VMs avec Vagrant :
   ```
   cd vagrant
   vagrant up
   ```

2. Déployer Kubernetes et tous les services :
   ```
   cd ..
   ansible-playbook playbooks/main.yml
   ```

3. Vérifier l'installation :
   ```
   ansible-playbook playbooks/test.yml
   ```

## Composants installés

- Kubernetes (v1.37.0)
- Calico (réseau, v3.32.2)
- Metrics Server (v0.9.0)
- MetalLB (load balancer, v0.16.1)
- Longhorn (stockage distribué répliqué)
- Kafka et Zookeeper (désactivé par défaut, voir `playbooks/main.yml`)
- keepalived (VIP haute disponibilité pour le plan de contrôle)

## Haute disponibilité du plan de contrôle

Le cluster utilise 3 masters (`master1`, `master2`, `master3`) avec etcd en topologie stacked : chaque master fait tourner sa propre instance etcd, répliquée entre les 3 nœuds. Avec 3 membres, le cluster etcd tolère la perte d'un nœud sans interruption (contrairement à une topologie à 2 masters, qui ne tolère aucune perte).

Un VIP (adresse IP virtuelle) `192.168.56.10:6443`, géré par **keepalived** (VRRP en unicast) sur les 3 masters, sert de point d'entrée unique pour l'API Kubernetes. En cas de panne du master qui détient le VIP, celui-ci bascule automatiquement (en quelques secondes) vers un autre master sain, selon la priorité :

| Master  | Priorité | Rôle initial |
|---------|----------|--------------|
| master1 | 150      | MASTER       |
| master2 | 140      | BACKUP       |
| master3 | 130      | BACKUP       |

`controlPlaneEndpoint` et les certificats du cluster (SANs) pointent sur ce VIP, jamais sur l'IP d'un master en particulier.

## Sauvegardes automatiques d'etcd

Chaque master exécute un timer systemd (`etcd-snapshot.timer`, configurable via `etcd_backup_schedule` dans `group_vars/masters.yml`, horaire par défaut) qui prend un snapshot etcd via `etcdctl snapshot save`, vérifie son intégrité avec `etcdutl snapshot status`, puis le publie sur le dossier partagé `/vagrant/etcd-backups/<nom-du-master>/` — c'est-à-dire directement sur le disque de la machine hôte, donc les sauvegardes survivent même si une VM est détruite ou corrompue. La rétention par hôte est configurable via `etcd_backup_retention_count` (24 par défaut, soit ~1 jour d'historique).

Pour lister les sauvegardes disponibles :
```
ls vagrant/etcd-backups/master1/ vagrant/etcd-backups/master2/ vagrant/etcd-backups/master3/
```

Pour restaurer un snapshot en cas de corruption (voir la documentation officielle [etcd disaster recovery](https://etcd.io/docs/latest/op-guide/recovery/) pour la procédure complète) :
```
etcdutl snapshot restore <fichier.db> --data-dir /var/lib/etcd-restored ...
```

## Accès au cluster

Le cluster expose son API via le VIP haute disponibilité, pas via l'IP d'un master en particulier. Pour accéder au cluster depuis la machine hôte, copiez le fichier de configuration depuis n'importe quel master (ex. `master1`) :
```
mkdir -p ~/.kube
scp -i vagrant/.vagrant/machines/master1/virtualbox/private_key vagrant@192.168.56.11:/home/vagrant/.kube/config ~/.kube/config
```

Le fichier récupéré pointe déjà vers `https://192.168.56.10:6443` (le VIP) et reste valide même si `master1` tombe, tant qu'au moins un autre master est disponible.

⚠️ Chaque reconstruction complète du cluster (`kubeadm reset` + réinitialisation) génère une **nouvelle autorité de certification** : un ancien `~/.kube/config` doit être régénéré après un rebuild, sous peine d'erreurs `x509: certificate signed by unknown authority`.

# Kafka sur Kubernetes - Guide d'utilisation

Ce guide présente les commandes essentielles pour vérifier, tester et gérer votre installation Kafka sur Kubernetes.

## Vérification de l'installation

Pour vérifier que les volumes persistants (PVC) ont été correctement créés :

```bash
kubectl get pvc -n kafka -l app.kubernetes.io/instance=kafka
```

## Test de fonctionnalité

### Création d'un pod de test

Pour tester la production et la consommation de messages, créez un pod temporaire :

```bash
kubectl run kafka-test --image=bitnami/kafka:latest --rm -it --restart=Never -n kafka -- bash
```

### Gestion des topics

Une fois connecté au pod, vous pouvez gérer les topics Kafka :

#### Créer un nouveau topic

```bash
kafka-topics.sh --create --topic test-topic --bootstrap-server kafka.kafka.svc.cluster.local:9092 --replication-factor 3 --partitions 3
```

#### Lister les topics existants

```bash
kafka-topics.sh --list --bootstrap-server kafka.kafka.svc.cluster.local:9092
```

#### Afficher les détails d'un topic

```bash
kafka-topics.sh --describe --topic test-topic --bootstrap-server kafka.kafka.svc.cluster.local:9092
```

### Production et consommation de messages

#### Produire des messages

```bash
kafka-console-producer.sh --topic test-topic --bootstrap-server kafka.kafka.svc.cluster.local:9092
# Tapez vos messages puis utilisez Ctrl+D pour quitter
```

#### Consommer des messages

```bash
kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server kafka.kafka.svc.cluster.local:9092
# Les messages produits devraient s'afficher ici
```

## Maintenance et surveillance

### Vérifier les groupes de consommateurs

```bash
kafka-consumer-groups.sh --list --bootstrap-server kafka.kafka.svc.cluster.local:9092
```

### Vérifier la configuration du broker

```bash
kafka-configs.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --entity-type brokers --entity-name 0 --describe
```

### Vérifier la connexion à Zookeeper

```bash
kafka-topics.sh --list --bootstrap-server kafka.kafka.svc.cluster.local:9092 --zookeeper zookeeper-0.zookeeper-headless.kafka.svc.cluster.local:2181
```

## Désinstallation

Pour désinstaller complètement Kafka de votre cluster Kubernetes :

### Désinstaller Kafka avec Helm

```bash
helm uninstall kafka -n kafka
```

### Supprimer les volumes persistants

```bash
kubectl -n kafka delete pvc --selector="app.kubernetes.io/instance=kafka"
```

### Désinstaller Zookeeper (si nécessaire)

```bash
helm uninstall zookeeper -n kafka
kubectl -n kafka delete pvc --selector="app.kubernetes.io/instance=zookeeper"
```

## Remarques importantes

- Assurez-vous d'avoir suffisamment de ressources disponibles dans votre cluster pour Kafka et Zookeeper
- La configuration avec 3 partitions et un facteur de réplication de 3 est recommandée pour les environnements de production
- Adaptez les commandes selon votre configuration spécifique (namespace, noms des services, etc.)