variable "confluent_cloud_api_key" {
  type      = string
  sensitive = true
}
variable "confluent_cloud_api_secret" {
  type      = string
  sensitive = true
}
variable "cloud_provider" {
  type    = string
  default = "AWS"
}
variable "region" {
  type    = string
  default = "us-east-1"
}
variable "environment_name" {
  type    = string
  default = "payments-lab"
}
variable "cluster_name" {
  type    = string
  default = "payments-basic"
}
variable "cluster_availability" {
  type    = string
  default = "SINGLE_ZONE"
}
