resource "google_project_service" "workflows" {
  service            = "workflows.googleapis.com"
  disable_on_destroy = false
  project         = var.project_id
}
resource "google_service_account" "workflows_service_account" {
  account_id   = "workflows-service-account"
  display_name = "Workflows Service Account"
  project      = var.project_id
}

################ les OUTPUTs

output "service_account" {
  description = "le service account"
  value = "${google_service_account.workflows_service_account}"
}

resource "google_workflows_workflow" "store_wkf" {
  #project        = var.project_id
  name            = "store_wkf"
  region          = var.region
  description     = "A sample workflow"
  service_account = google_service_account.workflows_service_account.id
  source_contents = file("../cloud_workflows/store_wkf.yaml")
  depends_on = [
    google_project_service.workflows,
    google_service_account.workflows_service_account,
    google_project_iam_member.storage_object_viewer,
    google_project_iam_member.bigquery_data_editor,
    google_project_iam_member.bigquery_job_user
  ]
}

# Ajouter les rôles IAM au compte de service
# Après
resource "google_project_iam_member" "storage_object_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.workflows_service_account.email}"
  depends_on = [
    google_service_account.workflows_service_account
  ]
}

resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.workflows_service_account.email}"
  depends_on = [
    google_service_account.workflows_service_account
  ]
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.workflows_service_account.email}"
  depends_on = [
    google_service_account.workflows_service_account
  ]
}

resource "google_project_iam_member" "logging_writer" {
  project = var.project_id  # Remplacez par l'ID de votre projet
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.workflows_service_account.email}"
}




# Service Account
output "workflows_service_account" {
  description = "Le compte de service pour les Workflows."
  value       = google_service_account.workflows_service_account
}

output "workflows_service_account_id" {
  description = "L'ID du compte de service pour les Workflows."
  value       = google_service_account.workflows_service_account.id
}

output "workflows_service_account_email" {
  description = "L'email du compte de service pour les Workflows."
  value       = google_service_account.workflows_service_account.email
}

# Workflow
output "store_wkf" {
  description = "Le workflow 'store_wkf'."
  value       = google_workflows_workflow.store_wkf
}

output "store_wkf_name" {
  description = "Le nom du workflow 'store_wkf'."
  value       = google_workflows_workflow.store_wkf.name
}

output "store_wkf_state" {
  description = "L'état du workflow 'store_wkf'."
  value       = google_workflows_workflow.store_wkf.state
}

# Rôles IAM
output "storage_object_viewer_role" {
  description = "Le rôle 'storage.objectViewer' attribué au compte de service."
  value       = google_project_iam_member.storage_object_viewer
}

output "bigquery_data_editor_role" {
  description = "Le rôle 'bigquery.dataEditor' attribué au compte de service."
  value       = google_project_iam_member.bigquery_data_editor
}

output "bigquery_job_user_role" {
  description = "Le rôle 'bigquery.jobUser' attribué au compte de service."
  value       = google_project_iam_member.bigquery_job_user
}

# API Workflows
output "workflows_api" {
  description = "L'API Workflows activée pour le projet."
  value       = google_project_service.workflows
}
