TOPICS := $(shell python -m kafka_tutorial.settings | awk '{print "topic-"$$1}')

.PHONY: topics

topics: $(TOPICS)

topic-%:
	@docker compose exec broker \
		kafka-topics --create \
		--topic $* \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 1 | grep 'Created topic' || echo "Couldn't create $*"
