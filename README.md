# Pipeline de Machine Learning en Temps Réel avec l’Architecture Kappa

## Table des matières

1. [Présentation du projet](#1-présentation-du-projet)
2. [Architecture du pipeline](#2-architecture-du-pipeline)
3. [Installation et configuration](#3-installation-et-configuration)
   - [3.1 Prérequis](#31-prérequis)
   - [3.2 Clonage du dépôt](#32-clonage-du-dépôt)
   - [3.3 Démarrage des conteneurs](#33-démarrage-des-conteneurs)
   - [3.4 Configuration de Spark](#34-configuration-de-spark)
4. [Exécution du pipeline](#4-exécution-du-pipeline)
   - [4.1 Configuration de Kafka UI](#41-configuration-de-kafka-ui)
   - [4.2 Démarrage du consommateur Spark](#42-démarrage-du-consommateur-spark)
   - [4.3 Démarrage du producteur Kafka](#43-démarrage-du-producteur-kafka)
   - [4.4 Suivi des performances du modèle](#44-suivi-des-performances-du-modèle)
   - [4.5 Visualisation des prédictions](#45-visualisation-des-prédictions)
5. [Démonstration](#5-démonstration)
6. [Remarques](#6-remarques)

---

## 1. Présentation du projet

Ce projet met en œuvre un pipeline de Machine Learning en temps réel reposant sur l’architecture Kappa. Il permet de traiter des données relatives aux émissions de CO₂ et de produire des prédictions à partir d’un flux de données continu.

Le pipeline s’appuie sur les technologies suivantes :

- **FastAPI** : expose une API permettant de diffuser les données issues d’un fichier CSV.
- **Apache Kafka** : assure la transmission des données en continu.
- **Apache Spark** : traite les flux de données et exécute les traitements de Machine Learning.
- **Streamlit** : fournit des interfaces interactives pour visualiser les prédictions et suivre les performances du modèle.
- **Docker et Docker Compose** : facilitent le déploiement et l’exécution des différents services.

## 2. Architecture du pipeline

![Architecture Kappa du pipeline de Machine Learning](https://github.com/user-attachments/assets/4523dd2b-a882-4e1e-9da2-82d23dfa70d4)

Le pipeline suit une architecture Kappa dans laquelle les données sont traitées à partir d’un flux continu :

1. FastAPI met les données à disposition.
2. Kafka reçoit et transmet les messages via un topic dédié.
3. Spark consomme les messages et exécute les traitements nécessaires.
4. Les résultats et les métriques du modèle sont exploités par les interfaces Streamlit.

Cette architecture permet de centraliser le traitement des données en flux et de suivre les résultats du modèle au fil de leur arrivée.

## 3. Installation et configuration

### 3.1 Prérequis

Avant de commencer, assurez-vous de disposer des éléments suivants :

- Docker Desktop installé et démarré.
- Git installé.
- Un terminal permettant d’exécuter les commandes Docker et Git.

Vérifiez les installations avec les commandes suivantes :

```bash
docker --version
docker compose version
git --version
```

### 3.2 Clonage du dépôt

Clonez le dépôt GitHub, puis accédez au répertoire du projet :

```bash
git clone https://github.com/farnounef/co2.git
cd ml-pipeline-spark-kafka
```

### 3.3 Démarrage des conteneurs

Accédez au répertoire de l’API et construisez son image Docker :

```bash
cd api
docker build -t streaming-api .
```

Revenez ensuite à la racine du projet et démarrez les services :

```bash
cd ..
docker compose up -d
```

Docker Compose démarre les services définis dans le fichier de configuration, notamment Kafka, Zookeeper, Spark et l’API.

Pour vérifier leur état :

```bash
docker compose ps
```

### 3.4 Configuration de Spark

Ouvrez un terminal dans le conteneur Spark Master :

```bash
docker exec -it spark-master bash
```

Accédez au répertoire de travail :

```bash
cd /app
```

Installez les dépendances nécessaires, si elles ne sont pas déjà présentes :

```bash
pip install scikit-learn kafka-python streamlit
```

> Remarque : selon la configuration du conteneur, certaines dépendances peuvent nécessiter une installation préalable ou une configuration spécifique de l’environnement Python.

## 4. Exécution du pipeline

### 4.1 Configuration de Kafka UI

Ouvrez Kafka UI dans votre navigateur :

http://localhost:8080

Créez un topic nommé `streaming_data`, qui servira à transmettre les données entre le producteur et le consommateur.

### 4.2 Démarrage du consommateur Spark

Le consommateur Spark récupère les messages publiés dans Kafka et effectue les traitements associés.

Dans un terminal connecté au conteneur Spark Master, lancez :

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  --jars kafka-clients-3.8.0.jar \
  --driver-class-path kafka-clients-3.8.0.jar \
  kafka_consumer.py
```

Vérifiez que le consommateur démarre correctement et qu’il est en mesure de lire les messages du topic `streaming_data`.

### 4.3 Démarrage du producteur Kafka

Dans un second terminal connecté au conteneur Spark Master, exécutez :

```bash
spark-submit kafka_producer.py
```

Le producteur lit les données du fichier CSV et les publie progressivement dans Kafka afin qu’elles puissent être consommées par Spark.

### 4.4 Suivi des performances du modèle

Pour suivre l’évolution de la RMSE (Root Mean Squared Error), lancez l’interface Streamlit dédiée à l’évaluation :

```bash
python -m streamlit run streamlit-eval.py
```

Ouvrez ensuite l’adresse locale indiquée dans le terminal pour consulter l’interface et suivre l’évolution des performances du modèle.

### 4.5 Visualisation des prédictions

Pour consulter les prédictions relatives aux émissions de CO₂, lancez l’application Streamlit correspondante :

```bash
python -m streamlit run streamlit-prediction.py
```

Accédez à l’URL affichée dans le terminal pour visualiser les résultats.

## 5. Démonstration

Une démonstration du projet est disponible ci-dessous :

[**Voir la démonstration du pipeline**](https://github.com/user-attachments/assets/66d8cee7-d49a-4dd6-946d-9f937cbc2fb4)

## 6. Remarques

- **Ordre d’exécution :** démarrez le consommateur Spark avant le producteur Kafka afin que les messages puissent être traités dès leur publication.
- **Terminaux distincts :** exécutez le consommateur et le producteur dans des terminaux séparés.
- **Configuration des services :** vérifiez que les conteneurs sont actifs et que les ports nécessaires sont disponibles avant de lancer le pipeline.
- **Dépendances :** assurez-vous que les bibliothèques Python et les connecteurs Kafka nécessaires sont accessibles dans l’environnement Spark.
