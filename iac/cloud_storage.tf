resource "google_storage_bucket" "magasin_cie_landing" {
  project  = var.project_id
  name     = "${var.project_id}_magasin_cie_landing"
  location = var.location
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
  lifecycle_rule {
    condition {
      age = 1000
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "magasin_cie_utils" {
  project  = var.project_id
  name     = "${var.project_id}_magasin_cie_utils"
  location = var.location
}


resource "google_storage_bucket" "cloud_functions_sources" {
  project                     = var.project_id
  name                        = "${var.project_id}_cloud_functions_sources"
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true
}

# load le fichier dans le bucket
resource "google_storage_bucket_object" "cleaned_store_sql" {
  bucket   = google_storage_bucket.magasin_cie_utils.name
  name = "cleaned_store.sql"
  source = var.cleaned_store_sql
}

# load le fichier dans le bucket
resource "google_storage_bucket_object" "raw_store_json" {
  bucket   = google_storage_bucket.magasin_cie_utils.name
  name = "raw_store_json"
  source = var.raw_store_json
}

# load le fichier dans le bucket
resource "google_storage_bucket_object" "cleaned_store_json" {
  bucket   = google_storage_bucket.magasin_cie_utils.name
  name = "cleaned_store_json"
  source = var.cleaned_store_json
}

resource "google_storage_bucket_iam_member" "workflow_storage_access" {
  bucket = google_storage_bucket.magasin_cie_utils.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workflows_service_account.email}"
}

