.PHONY: topics

topics: topic-ECOMMERCE_NEW_ORDER topic-ECOMMERCE_SEND_EMAIL

topic-%:
	@docker compose exec broker \
		kafka-topics --create \
		--topic $* \
		--bootstrap-server localhost:9092 \
		--replication-factor 1 \
		--partitions 1 | grep 'Created topic' || echo "Couldn't create $*"
