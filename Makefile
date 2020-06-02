.PHONY: clean data lint requirements sync_data_to_s3 sync_data_from_s3 test
.DEFAULT_GOAL := help

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = int-insights-pan-bbc-churn-predictions
PROFILE = default
PROJECT_NAME = pan-bbc-churn-predictions
PYTHON_INTERPRETER = python3
ENV=int

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

ifeq ($(ENV),live)
ACCOUNT=657378245742
else ifeq ($(ENV),int)
ACCOUNT=639227811136
else ifeq ($(ENV),test)
ACCOUNT=639227811136
else
ACCOUNT="need to choose valid ENV, int, test or live"
endif


#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install Python Dependencies
requirements: test_environment
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

## Make Dataset
data: requirements
	$(PYTHON_INTERPRETER) src/data/make_dataset.py

## Delete all compiled Python files and virtualenv
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	#find . -type d -name "*.egg-info" -exec rm -rf {} \;
	rm -rf dist/
	rm -rf venv/
	rm -rf wheelhouse/

## Lint using flake8
lint:
	flake8 src

## Run tests

## Package dependencies to requirements.txt
package:
	cp -R src/ airflow/plugins/$(PROJECT_NAME)/src/
	cp python_modules.sh airflow/plugins/$(PROJECT_NAME)/
	zip --exclude=airflow/* --exclude=venv/* --exclude=.git/* --exclude=src/data/* --exclude=src/R/metastore_db/* -r airflow/plugins/$(PROJECT_NAME)/code.zip  --exclude src/spark/newsseg_allfeatures_ALL --exclude src/spark/*.csv -r airflow/plugins/$(PROJECT_NAME)/code.zip . .

## Upload Data to S3
#sync_data_to_s3:
#ifeq (default,$(PROFILE))
#	aws s3 sync data/ s3://$(BUCKET)/data/
#else
#	aws s3 sync data/ s3://$(BUCKET)/data/ --profile $(PROFILE)
#endif

## Download Data from S3
sync_data_from_s3:
ifeq (default,$(PROFILE))
	aws s3 sync s3://$(BUCKET)/data/ data/
else
	aws s3 sync s3://$(BUCKET)/data/ data/ --profile $(PROFILE)
endif

sync_data_to_s3:
ifeq (default,$(PROFILE))
	aws s3 sync data/output/ s3://$(BUCKET)/data/output/
else
	aws s3 sync data/output/ s3://$(BUCKET)/data/output/ --profile $(PROFILE)
endif

## Set up python interpreter environment
create_environment:
	#$(PYTHON_INTERPRETER) -m pip install -q virtualenv
	@echo ">>> Installing virtualenv if not already intalled."
	@bash -c "virtualenv venv --python=$(PYTHON_INTERPRETER)"
	@echo ">>> New virtualenv created. Activate with: \n(mac) source venv/bin/activate \n(Windows) venv\Scripts\activate"

## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py

## Build R docker container
build_r_docker:
	cd R-docker; ./docker-build.sh

create_environment: venv/bin/activate

venv/bin/activate: infrastructure/requirements.txt
	# Create venv folder if doesn't exist. Run make clean to start over.
	test -d venv || $(PYTHON_INTERPRETER) -m virtualenv venv
	. venv/bin/activate; \
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel; \
	$(PYTHON_INTERPRETER) -m pip install -r  infrastructure/requirements.txt
	@echo ">>> virtualenv created/updated. Activate with: \n(mac) source venv/bin/activate \n(Windows) venv\Scripts\activate"
	touch venv/bin/activate

## Run tests
test: create_environment
	. venv/bin/activate; \
	$(PYTHON_INTERPRETER) -m unittest discover test


## Make json files from troposphere code
compile_stacks: create_environment
	. venv/bin/activate; \
	$(PYTHON_INTERPRETER) infrastructure/airflow_batch_stack.py

## Deploy aws stacks to specified environment, pass e.g. make deploy_stacks ENV=int
deploy_stacks: compile_stacks
	@echo "Deploying cloudformation to $(ENV): ${ACCOUNT}"
	aws cloudformation deploy --template-file infrastructure/airflow_batch_stack.json --stack-name $(ENV)-news-segmentation-batch --parameter-overrides $$(cat infrastructure/params/$(ENV)-batch.properties) --capabilities CAPABILITY_NAMED_IAM

## Build and deploy docker image
deploy_r_docker:
	@echo "Deploying docker image to $(ENV): ${ACCOUNT}"
	docker login https://${ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com -u AWS -p $$(aws ecr get-login --region eu-west-1 --registry-ids ${ACCOUNT} | cut -d' ' -f6); \
	cd R-docker; \
	./docker-build.sh; \
	IMAGE="pan-bbc-churn-predictions"; \
	TAG="0.0.1"; \
	docker tag $${IMAGE} ${ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com/$${IMAGE}:$${TAG};\
	docker push ${ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com/$${IMAGE}:$${TAG}

check_deploy:
	@echo "Deploying infrastructure to $(ENV): ${ACCOUNT}"
	@echo -n "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]

## Deploy all infrastructure to specified environment, pass e.g. make deploy_infra ENV=int
deploy_infra: check_deploy deploy_stacks deploy_r_docker
	@echo "Done!"


#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
