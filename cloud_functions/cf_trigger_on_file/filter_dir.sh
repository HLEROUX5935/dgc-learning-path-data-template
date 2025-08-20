#!/bin/bash
pwd
rsync -av --exclude='venv/' ../cloud_functions/cf_trigger_on_file/src/ ../cloud_functions/cf_trigger_on_file/src_filtered/
