#!/bin/bash

echo "Démarrage du déploiement du cluster Kubernetes"

echo "1. Démarrage des machines virtuelles avec Vagrant"
cd vagrant
vagrant up
if [ $? -ne 0 ]; then
  echo "Erreur lors du démarrage des VMs. Vérifiez les logs Vagrant."
  exit 1
fi
cd ..

echo "2. Vérification de la connectivité SSH"
ansible all -m ping
if [ $? -ne 0 ]; then
  echo "Erreur de connectivité SSH. Vérifiez que toutes les VMs sont accessibles."
  exit 1
fi

echo "3. Déploiement du cluster Kubernetes"
ansible-playbook playbooks/main.yml #--start-at-task="Vérification de l'adresse IP du service"

echo "4. Tests de validation"
ansible-playbook playbooks/test.yml

echo "Déploiement terminé ! Votre cluster Kubernetes est prêt."
echo "Pour accéder au cluster depuis cette machine :"
echo "mkdir -p ~/.kube"
echo "scp -i ~/.vagrant.d/insecure_private_key vagrant@192.168.56.11:/home/vagrant/.kube/config ~/.kube/config"
echo "kubectl get nodes"
