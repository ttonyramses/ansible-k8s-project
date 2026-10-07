# Cluster Kubernetes hautement disponible avec Ansible et Vagrant

Ce projet construit **de zéro et de façon automatisée** un cluster Kubernetes de laboratoire, hautement disponible, sur des machines virtuelles VirtualBox :

- **3 masters** (plan de contrôle) avec etcd répliqué et une **IP virtuelle (VIP)** pour l'API Kubernetes ;
- **3 workers** (nœuds applicatifs) équipés chacun d'un disque dédié au stockage ;
- les briques indispensables à un cluster utilisable : réseau des pods (**Calico**), métriques (**metrics-server**), IP externes pour les services (**MetalLB**), stockage persistant répliqué (**Longhorn**) ;
- des **sauvegardes automatiques d'etcd** stockées sur la machine hôte.

Vagrant crée les VMs, Ansible les configure. Une seule commande (`./deploy.sh`) enchaîne tout.

---

## Sommaire

1. [Architecture](#1-architecture)
2. [Prérequis](#2-prérequis)
3. [Démarrage rapide](#3-démarrage-rapide)
4. [Ce que fait le déploiement, étape par étape](#4-ce-que-fait-le-déploiement-étape-par-étape)
5. [Haute disponibilité : pourquoi 3 masters et pas 2](#5-haute-disponibilité--pourquoi-3-masters-et-pas-2)
6. [La VIP keepalived : un point d'entrée unique pour l'API](#6-la-vip-keepalived--un-point-dentrée-unique-pour-lapi)
7. [Comportement en cas de panne](#7-comportement-en-cas-de-panne)
8. [Sauvegardes et restauration d'etcd](#8-sauvegardes-et-restauration-detcd)
9. [Stockage persistant avec Longhorn](#9-stockage-persistant-avec-longhorn)
10. [Exposer des services avec MetalLB](#10-exposer-des-services-avec-metallb)
11. [Accès au cluster depuis la machine hôte](#11-accès-au-cluster-depuis-la-machine-hôte)
12. [Configuration (variables)](#12-configuration-variables)
13. [Structure du projet](#13-structure-du-projet)
14. [Commandes utiles](#14-commandes-utiles)
15. [Limites et points d'attention](#15-limites-et-points-dattention)
16. [Kafka](#16-kafka)

---

## 1. Architecture

```
                         Machine hôte (VirtualBox, réseau privé 192.168.56.0/24)
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │                                                                                   │
 │                    VIP API Kubernetes : 192.168.56.10:6443                        │
 │                     (portée par UN master à la fois, keepalived)                  │
 │                                       │                                           │
 │        ┌──────────────────────────────┼──────────────────────────────┐            │
 │        ▼                              ▼                              ▼            │
 │  ┌─────────────┐               ┌─────────────┐               ┌─────────────┐      │
 │  │  master1    │               │  master2    │               │  master3    │      │
 │  │ .56.11      │               │ .56.12      │               │ .56.16      │      │
 │  │ apiserver   │               │ apiserver   │               │ apiserver   │      │
 │  │ scheduler   │               │ scheduler   │               │ scheduler   │      │
 │  │ ctrl-manager│               │ ctrl-manager│               │ ctrl-manager│      │
 │  │ etcd ◄──────┼──── Raft ─────┼► etcd ◄─────┼──── Raft ─────┼► etcd       │      │
 │  │ keepalived  │               │ keepalived  │               │ keepalived  │      │
 │  └─────────────┘               └─────────────┘               └─────────────┘      │
 │                                                                                   │
 │  ┌─────────────┐               ┌─────────────┐               ┌─────────────┐      │
 │  │  node1      │               │  node2      │               │  node3      │      │
 │  │ .56.13      │               │ .56.14      │               │ .56.15      │      │
 │  │ kubelet     │               │ kubelet     │               │ kubelet     │      │
 │  │ Longhorn    │◄── réplique ─►│ Longhorn    │◄── réplique ─►│ Longhorn    │      │
 │  │ /dev/sdb 50G│               │ /dev/sdb 50G│               │ /dev/sdb 50G│      │
 │  └─────────────┘               └─────────────┘               └─────────────┘      │
 │                                                                                   │
 │  IP externes des services (MetalLB) : 192.168.56.200 → 192.168.56.220             │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

### Machines virtuelles (`vagrant/Vagrantfile`)

| VM      | IP              | RAM   | vCPU | Disques                          | Rôle                 |
|---------|-----------------|-------|------|----------------------------------|----------------------|
| master1 | 192.168.56.11   | 4 Go  | 2    | système                          | master (initial)     |
| master2 | 192.168.56.12   | 4 Go  | 2    | système                          | master               |
| master3 | 192.168.56.16   | 4 Go  | 2    | système                          | master               |
| node1   | 192.168.56.13   | 6 Go  | 2    | système + 50 Go (`/dev/sdb`)     | worker + stockage    |
| node2   | 192.168.56.14   | 6 Go  | 2    | système + 50 Go (`/dev/sdb`)     | worker + stockage    |
| node3   | 192.168.56.15   | 6 Go  | 2    | système + 50 Go (`/dev/sdb`)     | worker + stockage    |

> master3 est en `.16` (et non `.14`) parce qu'il a été ajouté après les workers.

Système : Ubuntu 26.04 (box `bento/ubuntu-26.04`).

### Plan d'adressage

| Plage / adresse                 | Usage                                                   |
|---------------------------------|---------------------------------------------------------|
| `192.168.56.10`                 | VIP de l'API Kubernetes (keepalived)                    |
| `192.168.56.11-16`              | IP des VMs                                              |
| `192.168.56.200-220`            | IP externes attribuées par MetalLB aux services `LoadBalancer` |
| `10.244.0.0/16`                 | IP des pods (Calico)                                    |
| `10.96.0.0/12`                  | IP internes des services (ClusterIP)                    |

### Versions

| Composant      | Version  |
|----------------|----------|
| Kubernetes     | 1.37.0   |
| containerd     | dernière version du dépôt Docker (puis gelée) |
| Calico         | v3.32.2  |
| metrics-server | v0.9.0   |
| MetalLB        | v0.16.1  |
| Helm           | v3.19.5  |
| Longhorn       | chart 1.12.1 |
| etcdctl/etcdutl| v3.7.0   |

---

## 2. Prérequis

Sur la machine hôte :

- **VirtualBox** (testé en 7.2 sur Apple Silicon, voir commentaire dans le `Vagrantfile`) ;
- **Vagrant** ;
- **Ansible** : `ansible-core` **2.15 ou plus** (le module `deb822_repository` l'exige) avec les collections `community.general` et `ansible.posix`. Elles sont incluses dans le paquet `ansible` complet ; sinon :
  ```bash
  ansible-galaxy collection install community.general ansible.posix
  ```
- **Ressources** : les VMs consomment à elles seules **30 Go de RAM**, **12 vCPU** et environ **210 Go de disque** (alloués dynamiquement). Une machine avec 32 Go de RAM est un minimum réaliste.
- Un accès Internet depuis les VMs (paquets, images, manifestes).

---

## 3. Démarrage rapide

### Tout en une commande

```bash
./deploy.sh
```

Le script :
1. démarre les 6 VMs (`vagrant up`) ;
2. vérifie qu'Ansible peut joindre chaque VM en SSH (`ansible all -m ping`) ;
3. lance le playbook complet (`playbooks/main.yml`) ;
4. lance les tests de validation (`playbooks/test.yml`).

### Ou pas à pas

```bash
cd vagrant && vagrant up && cd ..
ansible all -m ping
ansible-playbook playbooks/main.yml
ansible-playbook playbooks/test.yml
```

Compter en général 20 à 40 minutes selon la machine et la connexion.

### Détruire le cluster

```bash
cd vagrant && vagrant destroy -f
```

Les disques Longhorn (`vagrant/HardDisk/`) et les sauvegardes etcd (`vagrant/etcd-backups/`) sont sur l'hôte et **ne sont pas supprimés** par `vagrant destroy`.

---

## 4. Ce que fait le déploiement, étape par étape

`playbooks/main.yml` enchaîne les playbooks ci-dessous, dans cet ordre. Toute la logique est dans `playbooks/` ; le dossier `roles/` ne contient que des squelettes vides, sauf `wait-k8s-pods` (attente que les pods d'un namespace soient prêts).

| # | Playbook             | Cible            | Ce qu'il fait |
|---|----------------------|------------------|---------------|
| 0 | *(dans main.yml)*    | toutes           | Ping de toutes les VMs et collecte des informations système (facts). |
| 1 | `prerequisites.yml`  | toutes           | Prépare l'OS pour Kubernetes (détail ci-dessous). |
| 2 | `kubernetes.yml`     | toutes           | Installe `kubelet`, `kubeadm`, `kubectl` en version **1.37.0** depuis `pkgs.k8s.io`, les **gèle** (`apt hold`) pour éviter une mise à jour accidentelle, installe `crictl`, active l'autocomplétion `kubectl`. |
| 3 | `keepalived.yml`     | masters          | Installe et configure keepalived pour porter la VIP `192.168.56.10` ([section 6](#6-la-vip-keepalived--un-point-dentrée-unique-pour-lapi)). |
| 4 | `master_init.yml`    | master1          | Crée le cluster avec `kubeadm init` (détail ci-dessous). |
| 5 | `masters_join.yml`   | master2, master3 | Ajoute les deux autres masters au plan de contrôle (`kubeadm join --control-plane`). Chacun obtient son propre apiserver, scheduler, controller-manager **et membre etcd**. |
| 6 | `etcd_backup.yml`    | masters          | Installe `etcdctl`/`etcdutl` et un timer systemd de sauvegarde horaire ([section 8](#8-sauvegardes-et-restauration-detcd)). |
| 7 | `workers_join.yml`   | workers          | Ajoute node1-3 au cluster (`kubeadm join`). |
| 8 | `network.yml`        | master1          | Déploie **Calico** (réseau des pods) avec le CIDR `10.244.0.0/16`. Tant que Calico n'est pas installé, les nœuds restent en `NotReady` : c'est normal. |
| 9 | `metrics.yml`        | master1          | Déploie **metrics-server** (active `kubectl top` et l'autoscaling HPA). Option `--kubelet-insecure-tls` car les certificats des kubelets sont auto-signés. |
| 10| `loadbalancer.yml`   | master1          | Déploie **MetalLB** en mode L2 avec la plage `192.168.56.200-220`, puis crée un déploiement **nginx de test** exposé en `LoadBalancer` pour vérifier qu'une IP externe est bien attribuée. |
| 11| `longhorn.yml`       | master1          | Installe **Helm**, puis **Longhorn** via Helm avec **3 réplicas** par volume ([section 9](#9-stockage-persistant-avec-longhorn)). |
| — | `kafka.yml`          | master1          | **Désactivé** (commenté dans `main.yml`). Voir [section 16](#16-kafka). |
| 12| `test.yml`           | master1          | Affiche nœuds, pods, services, PVC ; teste l'IP MetalLB du nginx avec `curl` ; vérifie Longhorn et `kubectl top nodes`. |

### Détail de `prerequisites.yml`

Kubernetes a des exigences précises sur l'OS ; ce playbook les satisfait sur **toutes** les VMs :

- **swap désactivé** (immédiatement et dans `/etc/fstab`) : le kubelet refuse de démarrer avec du swap actif par défaut ;
- **modules noyau** `overlay` (système de fichiers des conteneurs) et `br_netfilter` (pour que le trafic des ponts réseau passe par iptables) ;
- **sysctl** : `ip_forward=1` et `bridge-nf-call-iptables=1`, indispensables au routage entre pods ;
- **containerd** (runtime de conteneurs) depuis le dépôt Docker, configuré avec `SystemdCgroup = true` (le kubelet et containerd doivent utiliser le même gestionnaire de cgroups) et l'image `pause:3.10` ;
- **chrony** : synchronisation des horloges. Important pour etcd et la validité des certificats TLS ;
- entrées `/etc/hosts` pour que chaque VM résolve les autres par leur nom ;
- **open-iscsi** et **nfs-common** : requis par Longhorn pour attacher les volumes ;
- sur les **workers uniquement** : formatage de `/dev/sdb` en ext4 et montage sur `/var/lib/longhorn`.

### Détail de `master_init.yml`

Génère un fichier `kubeadm-config.yaml` puis lance `kubeadm init --upload-certs`. Les points clés de la configuration :

- `controlPlaneEndpoint: 192.168.56.10:6443` : **le cluster est déclaré derrière la VIP**, et non derrière l'IP de master1. Tous les kubelets, kubeconfigs et composants parlent à la VIP ; c'est ce qui permet à master1 de tomber sans que le reste du cluster perde l'API ;
- `advertiseAddress` : l'IP propre du nœud (jamais la VIP), utilisée pour la communication entre masters ;
- `certSANs` : le certificat de l'apiserver est valide pour les noms et IP des 3 masters **et** pour la VIP, sinon les clients refuseraient la connexion après une bascule ;
- `--upload-certs` : les certificats du plan de contrôle sont stockés chiffrés dans le cluster, pour que master2 et master3 puissent les récupérer au moment du `join` (avec la *certificate key*).

Le playbook récupère ensuite la commande `kubeadm join` et la clé de certificats, et les garde **en mémoire dans Ansible** pour les étapes 5 et 7.

---

## 5. Haute disponibilité : pourquoi 3 masters et pas 2

L'intuition « avec 2 masters, si un tombe, l'autre prend le relais » est juste pour les composants sans état, mais **fausse pour etcd**, et c'est etcd qui impose le chiffre 3.

### Les composants du plan de contrôle n'ont pas tous le même mode de HA

| Composant                                  | Mode de HA                                                     | Fonctionne avec 2 masters ? |
|--------------------------------------------|----------------------------------------------------------------|-----------------------------|
| `kube-apiserver`                           | Actif sur tous les masters en même temps, sans état             | Oui                         |
| `kube-scheduler`, `kube-controller-manager`| Un seul actif (leader), élu via un verrou **stocké dans etcd**  | Oui, *tant qu'etcd marche*  |
| **etcd**                                   | Consensus **Raft** avec **quorum**                              | **Non**                     |

etcd est la base de données du cluster : tout l'état (pods, déploiements, secrets, etc.) y est stocké. Si etcd ne peut plus écrire, **plus rien ne peut changer dans le cluster**.

Ici, etcd est en topologie **stacked** : chaque master fait tourner son propre membre etcd, et les 3 membres se répliquent entre eux.

### La règle du quorum

Pour accepter une écriture ou élire un leader, etcd exige l'accord d'une **majorité stricte** de ses membres :

```
quorum = (N / 2) + 1     (division entière)
```

| Nombre de masters | Quorum | Pannes tolérées |
|-------------------|--------|-----------------|
| 1                 | 1      | 0               |
| **2**             | **2**  | **0**           |
| **3**             | **2**  | **1**           |
| 4                 | 3      | 1               |
| 5                 | 3      | 2               |

Avec **2 masters**, le quorum vaut 2. Si l'un tombe, le survivant est seul, et 1 sur 2 n'est pas une majorité : il **refuse toute écriture**. Le cluster est figé. Deux masters sont donc *moins* fiables qu'un seul, puisque deux fois plus de machines peuvent tomber et que la perte de n'importe laquelle bloque tout.

Avec **3 masters**, la perte d'un nœud laisse 2 membres sur 3, soit toujours une majorité. Le cluster continue normalement.

### Pourquoi le survivant ne prend-il pas simplement le relais ? Le *split-brain*

Quand un membre ne voit plus l'autre, il ne peut pas distinguer ces deux situations :

- l'autre est **réellement en panne** ;
- le **réseau** entre eux est coupé, et l'autre fonctionne toujours en pensant, lui aussi, être le survivant.

```
        coupure réseau
   ┌─────┐    ✂    ┌─────┐
   │  A  │─ ─ ─ ─ ─│  B  │
   └─────┘         └─────┘
  « B est mort,     « A est mort,
    je continue »     je continue »
        │                 │
  écrit « pod X     écrit « pod X
   sur node1 »       sur node2 »    →  deux vérités incompatibles
```

Si chacun continuait seul, on obtiendrait deux bases divergentes impossibles à réconcilier. Raft choisit la sécurité : **sans majorité, personne n'écrit**. Avec 3 membres, une coupure réseau laisse forcément un côté à 2 (majoritaire, qui continue) et un côté à 1 (minoritaire, qui s'arrête). Deux majorités ne peuvent pas exister en même temps.

### Pourquoi pas 4 ?

4 masters tolèrent 1 panne, exactement comme 3, pour une machine de plus. On choisit donc toujours un nombre **impair** : 3 pour tolérer 1 panne, 5 pour en tolérer 2.

---

## 6. La VIP keepalived : un point d'entrée unique pour l'API

Avoir 3 apiservers ne suffit pas : encore faut-il que les clients (kubectl, kubelets, pods) sachent **lequel contacter**. Si tout le monde pointait sur `192.168.56.11` (master1), la perte de master1 couperait l'accès à l'API alors que master2 et master3 fonctionnent.

La solution : une **IP virtuelle** (`192.168.56.10`) que **keepalived** (protocole VRRP) fait porter à **un seul master à la fois** et déplace automatiquement en cas de problème.

### Fonctionnement

- Les 3 masters exécutent keepalived et s'échangent des annonces VRRP chaque seconde, en **unicast** (directement d'IP à IP).
- Chaque master a une **priorité** ; celui de plus haute priorité porte la VIP :

  | Master  | Priorité de base | État initial |
  |---------|------------------|--------------|
  | master1 | 150              | MASTER       |
  | master2 | 140              | BACKUP       |
  | master3 | 130              | BACKUP       |

- Toutes les 3 secondes, un script (`/etc/keepalived/scripts/check_apiserver.sh`) teste `https://localhost:6443/healthz`. Après **2 échecs consécutifs**, la priorité du master baisse de **20**.

### Scénarios de bascule

- **master1 s'éteint** : il n'envoie plus d'annonces ; master2 (140) prend la VIP en quelques secondes.
- **master1 tourne mais son apiserver est cassé** : sa priorité passe de 150 à 130, sous master2 (140), qui prend la VIP. Ainsi, la VIP ne reste jamais sur un apiserver malade.
- **master1 revient** : sa priorité redevient 150 et il **reprend la VIP** (préemption, comportement par défaut de keepalived).

### Ce que la VIP ne fait *pas*

La VIP résout **l'accès** à l'API, pas la **cohérence des données**. Même si la VIP est portée par un master sain, l'API reste en lecture seule si etcd a perdu son quorum. Les deux mécanismes sont complémentaires :

| Problème                                      | Résolu par            |
|-----------------------------------------------|-----------------------|
| « À quelle adresse joindre l'API ? »          | VIP keepalived        |
| « Les données du cluster restent-elles cohérentes et modifiables ? » | 3 membres etcd (quorum) |
| « Et si etcd est perdu ou corrompu ? »        | Sauvegardes etcd      |

> Note : la VIP fait de la bascule (*failover*), pas de la répartition de charge. Toutes les requêtes passent par le master qui la détient. Pour un labo, c'est largement suffisant.

---

## 7. Comportement en cas de panne

| Panne                              | Conséquence |
|------------------------------------|-------------|
| **1 master**                       | Aucune interruption visible. La VIP bascule si nécessaire, etcd garde son quorum (2/3). |
| **2 masters**                      | etcd perd son quorum : l'**API est bloquée** (aucune création, suppression ni modification). Les applications déjà lancées sur les workers **continuent de tourner**, mais rien ne peut être redéployé et les pods en panne ne sont plus remplacés. Il faut redémarrer au moins un master ; sinon, restaurer etcd ([section 8](#8-sauvegardes-et-restauration-detcd)). |
| **3 masters**                      | Même chose, en pire : plus d'API du tout. Les pods déjà lancés continuent. |
| **1 worker**                       | Après ~5 minutes (délai par défaut de Kubernetes), ses pods sont recréés sur les autres workers. Les volumes Longhorn restent accessibles via leurs 2 autres réplicas. |
| **La machine hôte**                | Tout tombe : les 6 VMs sont sur la même machine physique (voir [limites](#15-limites-et-points-dattention)). |

---

## 8. Sauvegardes et restauration d'etcd

### Ce qui est installé (`etcd_backup.yml`)

Sur **chaque master** :

- `etcdctl` et `etcdutl` v3.7.0 dans `/usr/local/bin/` (même version que l'etcd du plan de contrôle) ;
- le script `/usr/local/sbin/etcd-snapshot.sh`, qui :
  1. prend un snapshot du membre etcd local (`etcdctl snapshot save`) ;
  2. **vérifie son intégrité** (`etcdutl snapshot status`) ;
  3. le déplace vers `/vagrant/etcd-backups/<master>/etcd-snapshot-AAAAMMJJ-HHMMSS.db` ;
  4. ne conserve que les **24 plus récents** ;
- un service et un timer systemd (`etcd-snapshot.service` / `etcd-snapshot.timer`), déclenchés **toutes les heures** (avec un décalage aléatoire de 60 s maximum, et un rattrapage au démarrage si une exécution a été manquée).

Une première sauvegarde est lancée immédiatement pendant le déploiement pour valider la chaîne.

`/vagrant` est le dossier `vagrant/` du projet, **partagé avec la machine hôte**. Les sauvegardes sont donc physiquement sur l'hôte (`vagrant/etcd-backups/`, ignoré par Git) et survivent à la destruction des VMs.

### Consulter les sauvegardes

```bash
ls -lh vagrant/etcd-backups/*/
```

Sur un master :

```bash
systemctl list-timers etcd-snapshot.timer      # prochaine exécution
journalctl -u etcd-snapshot.service -n 50      # logs des dernières sauvegardes
sudo systemctl start etcd-snapshot.service     # forcer une sauvegarde maintenant
```

### Quand faut-il restaurer ?

| Situation                                   | Restaurer un snapshot ? |
|---------------------------------------------|-------------------------|
| 1 master perdu, les 2 autres fonctionnent   | **Non** : le quorum est intact, il suffit de **remplacer** le master (procédure A). |
| 2 ou 3 masters perdus définitivement, ou données etcd corrompues, ou suppression accidentelle massive | **Oui** (procédure B). |

Dans les commandes ci-dessous, on définit un raccourci pour `etcdctl` (à exécuter **sur un master**, en root) :

```bash
alias e='ETCDCTL_API=3 etcdctl --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/server.crt \
  --key=/etc/kubernetes/pki/etcd/server.key'

e member list -w table
e endpoint status --cluster -w table
e endpoint health --cluster
```

### Procédure A : remplacer un master perdu (quorum intact)

Exemple avec master2 hors service :

```bash
# 1. Depuis un master sain : retirer le nœud et son membre etcd
kubectl delete node master2
e member list -w table          # repérer l'ID du membre "master2"
e member remove <ID>

# 2. Sur master2 (ou une VM neuve recréée avec vagrant) : nettoyer
sudo kubeadm reset -f

# 3. Depuis master1 : générer une commande join et une clé de certificats neuves
kubeadm token create --print-join-command
sudo kubeadm init phase upload-certs --upload-certs   # dernière ligne = clé

# 4. Sur master2 : rejoindre comme master
sudo <commande join> --control-plane --certificate-key <clé> \
  --apiserver-advertise-address=192.168.56.12
```

Retirer le membre etcd **avant** de réintégrer le master est essentiel : sinon, etcd compte un membre fantôme et le quorum devient plus fragile.

### Procédure B : restaurer un snapshot (perte du quorum)

La restauration remet **tout le cluster** dans l'état du snapshot. Il faut la faire sur **les 3 masters**, **avec le même fichier**. Comme `/vagrant` est partagé, les 3 masters voient les mêmes snapshots.

```bash
# 1. Sur CHAQUE master : arrêter le plan de contrôle (kubelet arrête les pods statiques)
sudo mkdir -p /root/manifests-backup
sudo mv /etc/kubernetes/manifests/*.yaml /root/manifests-backup/

# 2. Sur CHAQUE master : mettre de côté l'ancien répertoire de données
sudo mv /var/lib/etcd /var/lib/etcd.old

# 3. Sur CHAQUE master : restaurer le même snapshot,
#    en adaptant --name et --initial-advertise-peer-urls au master courant
SNAP=/vagrant/etcd-backups/master1/etcd-snapshot-AAAAMMJJ-HHMMSS.db
NAME=master1          # master2 / master3
IP=192.168.56.11      # 192.168.56.12 / 192.168.56.16

sudo etcdutl snapshot restore "$SNAP" \
  --name "$NAME" \
  --initial-cluster master1=https://192.168.56.11:2380,master2=https://192.168.56.12:2380,master3=https://192.168.56.16:2380 \
  --initial-cluster-token etcd-cluster-restore \
  --initial-advertise-peer-urls "https://$IP:2380" \
  --data-dir /var/lib/etcd

# 4. Sur CHAQUE master : relancer le plan de contrôle
sudo mv /root/manifests-backup/*.yaml /etc/kubernetes/manifests/

# 5. Vérifier
e member list -w table
kubectl get nodes
```

Le nom (`--name`) doit correspondre au nom du nœud, car c'est le nom que kubeadm donne au membre etcd. Pour les cas particuliers, voir la [documentation officielle etcd (disaster recovery)](https://etcd.io/docs/latest/op-guide/recovery/) et la [documentation kubeadm](https://kubernetes.io/docs/tasks/administer-cluster/configure-upgrade-etcd/#restoring-an-etcd-cluster).

> Un snapshot ne contient **que l'état Kubernetes** (objets, secrets, etc.), pas les **données applicatives** stockées dans les volumes Longhorn.

---

## 9. Stockage persistant avec Longhorn

Les pods sont éphémères : quand un pod est recréé, ses fichiers locaux disparaissent. Pour les applications qui doivent conserver des données (bases de données, Kafka, etc.), Kubernetes utilise des **volumes persistants (PVC)**. **Longhorn** fournit ces volumes à partir des disques des workers.

- Chaque worker a un **disque dédié de 50 Go** (`/dev/sdb`, fichier `vagrant/HardDisk/nodeX_disk2.vdi` sur l'hôte), formaté en ext4 et monté sur `/var/lib/longhorn`.
- Chaque volume est **répliqué 3 fois** (`storage_replication_factor: 3`), une copie par worker. Si un worker tombe, le volume reste disponible grâce aux 2 autres copies.
- Longhorn crée une **StorageClass `longhorn`** à utiliser dans les PVC :

  ```yaml
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: mes-donnees
  spec:
    accessModes: [ReadWriteOnce]
    storageClassName: longhorn
    resources:
      requests:
        storage: 5Gi
  ```

> Avec 3 workers et 3 réplicas, la perte d'un worker laisse le volume en état **Degraded** (2 copies sur 3) : il fonctionne, mais Longhorn ne peut pas recréer la 3ᵉ copie ailleurs tant que le worker n'est pas revenu.

Interface web de Longhorn (depuis la machine hôte, une fois le kubeconfig récupéré) :

```bash
kubectl -n longhorn-system port-forward svc/longhorn-frontend 8080:80
# puis http://localhost:8080
```

---

## 10. Exposer des services avec MetalLB

Sur un cloud public, un service de type `LoadBalancer` reçoit automatiquement une IP externe fournie par le cloud. Sur des VMs locales, personne ne la fournit, et le service resterait indéfiniment en `<pending>`. **MetalLB** joue ce rôle :

- il attribue au service une IP libre de la plage `192.168.56.200-220` ;
- en mode **L2**, l'un des nœuds répond aux requêtes ARP pour cette IP, qui devient joignable depuis la machine hôte.

Le déploiement crée un **nginx de test** pour le vérifier :

```bash
kubectl get svc nginx-service        # colonne EXTERNAL-IP, ex. 192.168.56.200
curl http://192.168.56.200           # page d'accueil nginx, depuis l'hôte
```

Vous pouvez le supprimer une fois le cluster validé :

```bash
kubectl delete svc nginx-service && kubectl delete deployment nginx
```

---

## 11. Accès au cluster depuis la machine hôte

Copier le kubeconfig depuis n'importe quel master (Vagrant génère une clé SSH par VM) :

```bash
mkdir -p ~/.kube
scp -i vagrant/.vagrant/machines/master1/virtualbox/private_key \
  vagrant@192.168.56.11:/home/vagrant/.kube/config ~/.kube/config
kubectl get nodes
```

Le fichier pointe sur `https://192.168.56.10:6443` (la VIP). Il reste donc valide même si master1 tombe, tant qu'un autre master est disponible.

Pour se connecter en SSH à une VM :

```bash
cd vagrant && vagrant ssh master1
```

> ⚠️ Chaque reconstruction complète du cluster (`vagrant destroy` ou `kubeadm reset`, puis redéploiement) génère une **nouvelle autorité de certification**. L'ancien `~/.kube/config` provoque alors `x509: certificate signed by unknown authority` : il faut le récupérer à nouveau.

---

## 12. Configuration (variables)

### `group_vars/all.yml` (toutes les machines)

| Variable                      | Défaut                        | Rôle |
|-------------------------------|-------------------------------|------|
| `kubernetes_version`          | `1.37.0`                      | Version exacte des paquets kubelet/kubeadm/kubectl |
| `kubernetes_minor_version`    | `1.37`                        | Dépôt apt `pkgs.k8s.io` utilisé |
| `pod_network_cidr`            | `10.244.0.0/16`               | Plage d'IP des pods |
| `service_network_cidr`        | `10.96.0.0/12`                | Plage d'IP des services |
| `calico_version`              | `v3.32.2`                     | |
| `metrics_server_version`      | `v0.9.0`                      | |
| `metallb_version`             | `v0.16.1`                     | |
| `metallb_ip_range`            | `192.168.56.200-192.168.56.220` | IP externes des services `LoadBalancer` |
| `helm_version`                | `v3.19.5`                     | |
| `longhorn_namespace`          | `longhorn-system`             | |
| `longhorn_chart_version`      | `1.12.1`                      | |
| `storage_replication_factor`  | `3`                           | Nombre de copies de chaque volume Longhorn |
| `kafka_*`, `zookeeper_*`      |                               | Voir `kafka_user_manual.md` |

### `group_vars/masters.yml`

| Variable                       | Défaut               | Rôle |
|--------------------------------|----------------------|------|
| `apiserver_vip`                | `192.168.56.10`      | VIP de l'API Kubernetes |
| `vrrp_router_id`               | `51`                 | Identifiant du groupe VRRP (doit être unique sur le réseau) |
| `vrrp_auth_pass`               | *(labo)*             | Mot de passe partagé entre les keepalived |
| `etcd_version`                 | `v3.7.0`             | Version d'etcdctl/etcdutl (doit correspondre à l'etcd du cluster) |
| `etcd_backup_schedule`         | `hourly`             | Fréquence des sauvegardes (syntaxe systemd `OnCalendar`, ex. `*:0/15` pour toutes les 15 min) |
| `etcd_backup_retention_count`  | `24`                 | Nombre de snapshots conservés par master |

### `group_vars/workers.yml`

| Variable             | Défaut              | Rôle |
|----------------------|---------------------|------|
| `longhorn_disk_path` | `/dev/sdb`          | Disque dédié à Longhorn |
| `longhorn_data_path` | `/var/lib/longhorn` | Point de montage |

### Autres fichiers

- `inventory.ini` : liste des machines, groupes `masters`, `workers` et `primary_master` (le master qui initialise le cluster et sur lequel s'exécutent les déploiements `kubectl`/Helm).
- `ansible.cfg` : inventaire par défaut, utilisateur `vagrant`, cache des facts dans `./facts_cache`, pipelining SSH.
- `vagrant/Vagrantfile` : définition des VMs (IP, RAM, CPU, disques secondaires).

> Pour changer une IP, il faut la modifier de façon cohérente dans `vagrant/Vagrantfile`, `inventory.ini`, les `certSANs` de `playbooks/master_init.yml` et les entrées `/etc/hosts` de `playbooks/prerequisites.yml`.

---

## 13. Structure du projet

```
.
├── README.md                  # Ce fichier
├── kafka_user_manual.md       # Guide d'utilisation de Kafka
├── deploy.sh                  # Déploiement complet en une commande
├── ansible.cfg                # Configuration Ansible
├── inventory.ini              # Inventaire des machines
├── group_vars/
│   ├── all.yml                # Variables globales (versions, réseaux, stockage)
│   ├── masters.yml            # VIP, keepalived, sauvegardes etcd
│   └── workers.yml            # Disque Longhorn
├── vagrant/
│   ├── Vagrantfile            # Définition des 6 VMs
│   ├── HardDisk/              # (généré) disques Longhorn des workers
│   └── etcd-backups/          # (généré) snapshots etcd, par master
├── playbooks/
│   ├── main.yml               # Enchaîne tous les playbooks
│   ├── prerequisites.yml      # Préparation OS + containerd
│   ├── kubernetes.yml         # kubelet / kubeadm / kubectl
│   ├── keepalived.yml         # VIP HA de l'API
│   ├── master_init.yml        # kubeadm init sur master1
│   ├── masters_join.yml       # Ajout de master2 et master3
│   ├── etcd_backup.yml        # Sauvegardes etcd automatiques
│   ├── workers_join.yml       # Ajout des workers
│   ├── network.yml            # Calico
│   ├── metrics.yml            # metrics-server
│   ├── loadbalancer.yml       # MetalLB + nginx de test
│   ├── longhorn.yml           # Helm + Longhorn
│   ├── kafka.yml              # Zookeeper + Kafka (désactivé)
│   └── test.yml               # Vérifications finales
└── roles/                     # Squelettes ; seul wait-k8s-pods est utilisé
```

---

## 14. Commandes utiles

```bash
# État général
kubectl get nodes -o wide
kubectl get pods -A
kubectl top nodes

# Quel master porte la VIP ?
for m in master1 master2 master3; do
  (cd vagrant && vagrant ssh $m -c "ip -4 addr | grep -q 192.168.56.10 && echo $m porte la VIP" 2>/dev/null)
done

# Tester la haute disponibilité
(cd vagrant && vagrant halt master1)
kubectl get nodes                 # répond toujours (VIP sur master2)
(cd vagrant && vagrant up master1)

# Santé etcd (sur un master, avec l'alias "e" de la section 8)
e endpoint status --cluster -w table

# Relancer une seule étape (sur un cluster existant)
ansible-playbook playbooks/longhorn.yml
ansible-playbook playbooks/etcd_backup.yml
ansible-playbook playbooks/test.yml
```

---

## 15. Limites et points d'attention

Ce projet est un **environnement de laboratoire**. Les points suivants sont acceptables ici, mais pas en production :

- **Un seul hôte physique** : la HA protège contre la panne d'une VM ou d'un processus, pas contre celle de la machine hôte. En production, les masters sont répartis sur des machines, voire des zones, différentes.
- **Secret VRRP en clair** dans `group_vars/masters.yml` (à placer dans Ansible Vault sinon).
- **metrics-server en `--kubelet-insecure-tls`** : il ne vérifie pas les certificats des kubelets.
- **`prerequisites.yml` passe `/var/lib/apt/lists/` et `/var/lib/dpkg/` en `0777`** pour contourner des erreurs de verrou apt.
- **La commande `join` n'existe qu'en mémoire pendant l'exécution** : `masters_join.yml` et `workers_join.yml` ne fonctionnent qu'enchaînés après `master_init.yml` dans le même lancement (via `main.yml`). Pour ajouter un nœud plus tard, générer la commande à la main (voir procédure A, [section 8](#8-sauvegardes-et-restauration-detcd)). Le jeton est valable 24 h, la clé de certificats 2 h.
- **Kafka désactivé par défaut**, mais `test.yml` interroge quand même le namespace `kafka` : un résultat vide y est normal.

---

## 16. Kafka

L'installation de Zookeeper et Kafka (3 brokers, volumes Longhorn, service externe via MetalLB) est **désactivée par défaut**. Pour l'activer, décommenter le bloc correspondant dans `playbooks/main.yml`, ou lancer directement :

```bash
ansible-playbook playbooks/kafka.yml
```

Prévoir des ressources suffisantes : 3 brokers jusqu'à 2 Go de RAM chacun, plus 3 Zookeeper.

Le guide d'utilisation (tests, topics, production/consommation, désinstallation) est dans **[kafka_user_manual.md](kafka_user_manual.md)**.
