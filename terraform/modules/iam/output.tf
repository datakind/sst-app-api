output "cloudrun_service_account_email" {
  value = google_service_account.cloudrun_sa.email
}

output "cloudrun_service_account_id" {
  value = google_service_account.cloudrun_sa.id
}

output "cloudbuild_service_account_id" {
  value = google_service_account.cloudbuild_sa.id
}

output "cloudbuild_service_account_email" {
  value = google_service_account.cloudbuild_sa.email
}

output "terraform_service_account_id" {
  value = google_service_account.terraform_sa.id
}

output "iap_impersonation_service_account_id" {
  value = google_service_account.iap_impersonation_sa.id
}

output "iap_impersonation_sa_email" {
  value = google_service_account.iap_impersonation_sa.email
}
