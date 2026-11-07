output "environment_id" {
  value = confluent_environment.lab.id
}
output "cluster_id" {
  value = confluent_kafka_cluster.lab.id
}
output "producer_service_account_id" {
  value = confluent_service_account.producer.id
}
output "consumer_service_account_id" {
  value = confluent_service_account.consumer.id
}
output "flink_service_account_id" {
  value = confluent_service_account.flink.id
}
output "cluster_bootstrap_endpoint" {
  value = confluent_kafka_cluster.lab.bootstrap_endpoint
}
output "producer_kafka_api_key" {
  value = confluent_api_key.producer.id
}
output "producer_kafka_api_secret" {
  value     = confluent_api_key.producer.secret
  sensitive = true
}
output "consumer_kafka_api_key" {
  value = confluent_api_key.consumer.id
}
output "consumer_kafka_api_secret" {
  value     = confluent_api_key.consumer.secret
  sensitive = true
}
output "flink_kafka_api_key" {
  value = confluent_api_key.flink_kafka.id
}
output "flink_kafka_api_secret" {
  value     = confluent_api_key.flink_kafka.secret
  sensitive = true
}
