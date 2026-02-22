all: workshop_resources.json

workshop_resources.json:
	aws cloudformation describe-stacks --stack-name workshop-resources --query "Stacks[0].Outputs" --output json \
  | jq 'map({(.OutputKey): .OutputValue}) | add' > workshop_resources.json

.PHONY: all clean
clean:
	rm -f workshop_resources.json