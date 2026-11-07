resource "confluent_environment" "lab" {
  display_name = var.environment_name
}

resource "confluent_kafka_cluster" "lab" {
  display_name = var.cluster_name
  availability = var.cluster_availability
  cloud        = var.cloud_provider
  region       = var.region
  basic {}
  environment {
    id = confluent_environment.lab.id
  }
}

resource "confluent_service_account" "producer" {
  display_name = "payments-producer"
  description  = "Identidade do connector CDC para publicar eventos de pagamento"
}

resource "confluent_service_account" "consumer" {
  display_name = "payments-consumer"
  description  = "Identidade de leitura para consumidores do pipeline"
}

resource "confluent_service_account" "flink" {
  display_name = "payments-flink"
  description  = "Identidade de processamento Flink para ler CDC e publicar alertas"
}

resource "confluent_api_key" "producer" {
  display_name = "payments-producer-kafka-key"
  owner {
    id          = confluent_service_account.producer.id
    api_version = confluent_service_account.producer.api_version
    kind        = confluent_service_account.producer.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind
    environment { id = confluent_environment.lab.id }
  }
}

resource "confluent_api_key" "consumer" {
  display_name = "payments-consumer-kafka-key"
  owner {
    id          = confluent_service_account.consumer.id
    api_version = confluent_service_account.consumer.api_version
    kind        = confluent_service_account.consumer.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind
    environment { id = confluent_environment.lab.id }
  }
}

resource "confluent_api_key" "flink_kafka" {
  display_name = "payments-flink-kafka-key"
  owner {
    id          = confluent_service_account.flink.id
    api_version = confluent_service_account.flink.api_version
    kind        = confluent_service_account.flink.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind
    environment { id = confluent_environment.lab.id }
  }
}

resource "confluent_kafka_acl" "producer_write" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "producer_describe" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "producer_create" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = "CREATE"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "producer_cluster_describe" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.producer.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "consumer_read" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.consumer.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "consumer_group_read" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "GROUP"
  resource_name = "payments-consumer-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.consumer.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "flink_source_read" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments.public."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.flink.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "flink_group_read" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "GROUP"
  resource_name = "flink-payments-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.flink.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "flink_alert_write" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments.fraud.alerts"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "flink_alert_describe" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "TOPIC"
  resource_name = "payments.fraud.alerts"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
}

resource "confluent_kafka_acl" "flink_cluster_create" {
  kafka_cluster { id = confluent_kafka_cluster.lab.id }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.flink.id}"
  host          = "*"
  operation     = "CREATE"
  permission    = "ALLOW"
}
