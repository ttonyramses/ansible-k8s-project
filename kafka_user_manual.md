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