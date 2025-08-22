# ======================================================================================== #
#                   ___  ___   ___ _____   __  __      _        __ _ _
#                  | _ \/ _ \ / _ \_   _| |  \/  |__ _| |_____ / _(_) |___
#                  |   / (_) | (_) || |   | |\/| / _` | / / -_)  _| | / -_)
#                  |_|_\\___/ \___/ |_|   |_|  |_\__,_|_\_\___|_| |_|_\___|
#
# ======================================================================================== #
# -- < Global configuration > --
# ======================================================================================== #
SHELL := /bin/bash

.SHELLFLAGS := -o pipefail -c
.DELETE_ON_ERROR:
.EXPORT_ALL_VARIABLES:
.DEFAULT_GOAL := help

SQUAD := offer
# ---------------------------------------------------------------------------------------- #
# This target will be called whenever make is called without any target. So this is the
# default target and must be the first declared.
# ---------------------------------------------------------------------------------------- #
define HERE_HELP
The available targets are:
--------------------------
help                      Displays the current message
-------------
-- < IaC > --
-------------
If any of the previous step has not been run, it will be executed before the called step
Check numbers below for a detailed dependency graph
Ex: [iac-apply] will call [iac-plan] and [iac-validate] if the "terraform plan" file is not up to date
--
iac-init                  1.  Initialize the terraform infrastructure
iac-workspace-select-dev  2.  Select the terraform workspace of the 'dev' environment
iac-validate              3.1 Validate the terraform files created
iac-plan                  3.2 Produce the terraform plan to visualize what will be changed in the infrastructure
iac-apply                 4.  Proceed to the application of the terraform infrastructure
--
iac-upgrade               Upgrade the providers during the initialisation of the terraform infrastructure
iac-clean                 Clean the intermediary terraform files to restart the process
iac-clean-plan            Clean the generated terraform plan and its validation
iac-reapply               Combine [iac-clean-plan] and [iac-apply] to restart from scratch
iac-replan                Combine [iac-clean-plan] and [iac-plan]  to restart from scratch
---------------
-- < Vault > --
---------------
[vault-refresh-token] will be called before [iac-plan] and [iac-apply]
--
vault-refresh-token       Refresh the token if the token is NOT up to date and is LESS than TWO hours old
vault-clean-token         Clean the token file used by terraform to retrieve the secret
---------------
endef
export HERE_HELP

.PHONY: help
help:
	@printf "=%.s" $$(seq 100)
	@echo ""
	@echo "-- Welcome to the root makefile help"
	@printf "=%.s" $$(seq 100)
	@echo ""
	@echo "$$HERE_HELP"
	@echo ""

# ---------------------------------------------------------------------------------------- #
# -- < IaC > --
# ---------------------------------------------------------------------------------------- #
# TF_VAR reconnu par terraform => is_makefile_call
TF_VAR_is_makefile_call := true

# -- terraform variables declaration
MAKE_DIR       := make_dir
TF_STATE       := $(MAKE_DIR)/terraform.tfstate
TF_ENVIRONMENT := $(MAKE_DIR)/environment.ctl
TF_VALIDATE    := $(MAKE_DIR)/tfvalidate.ctl
TF_PLAN        := $(MAKE_DIR)/tfplan.ctl

# -- terraform targets & dependencies
.PHONY: iac-rep
iac-rep:
	@echo "[iac-rep] :: création du répertoire $(MAKE_DIR)"
	@mkdir -p $(MAKE_DIR)

.PHONY: iac-init
iac-init: iac-rep $(TF_STATE)
$(TF_STATE):
	@echo "[iac-init] :: initialisation terraform"
	@terraform init
	
.PHONY: iac-upgrade
iac-upgrade: 
	@echo "[iac-upgrade] :: initializing & upgrading terraform"
	@terraform init -upgrade

.PHONY: iac-workspace-select-dev
iac-workspace-select-dev: $(TF_ENVIRONMENT)
$(TF_ENVIRONMENT): $(TF_STATE)
	@echo "[iac-workspace-select-dev] :: selecting dev environment"
	#@terraform workspace select dev

.PHONY: iac-validate
iac-validate: iac-clean-plan $(TF_VALIDATE)
$(TF_VALIDATE): $(TF_ENVIRONMENT)
	@echo "[iac-validate] :: checking terraform files syntax"
	@terraform validate | tee "$${TF_VALIDATE#*/}"

.PHONY: iac-plan
iac-plan: iac-clean-plan $(TF_PLAN)
$(TF_PLAN): $(TF_VALIDATE)
	@echo "[iac-plan] :: producing the terraform infrastructure plan"
	@terraform plan -out="$${TF_PLAN#*/}"
	@ls -lrt  $(MAKE_DIR)

.PHONY: iac-apply
iac-apply: $(TF_PLAN)
	@echo "[iac-apply] :: deploying the terraform infrastructure"
	@terraform apply -auto-approve -input=false "$${TF_PLAN#*/}"
	@ls -lrt  $(MAKE_DIR)

.PHONY: iac-clean-plan
iac-clean-plan: iac-rep
	@echo "[iac-clean-plan] :: cleaning the terraform plan and its validation"
	@rm -rf $(TF_PLAN)
	@rm -rf $(TF_VALIDATE)

.PHONY: iac-clean
iac-clean: iac-clean-plan 
	@echo "[iac-clean] :: cleaning the terraform infrastructure"
	@rm -rf $(MAKE_DIR)

.PHONY: iac-replan
iac-replan: iac-clean-plan iac-plan

.PHONY: iac-reapply
iac-reapply: iac-clean-plan iac-apply

