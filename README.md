# mini-Kafka - Broker

## Description

The mini-Kafka Message Broker is a lightweight, simple fault-tolerant messagin system inspired by
Apache Kafka. It is designed for simplicity and specially ease of use, making it an excellent choice
for small-scale projects with a quick minimal configuration settings.

### key features:

- Fault Tolerance: It gracefully handles failures upto one node, ensuring reliable message delivery
  even in adverse conditions.
- Scalability: The broker can handle increasing workloads by distributing messages across multiple
  nodes. It is able to be scale by adding more nodes to the cluster automatically.
- Minimal Configuration: Get started quickly with minimal configuration settings. Just run it!

## Installation

- For each of the master and broker projects, a CI is run after a PR is created and the tests are
  passed.
- If there are no problems and the PR is merged with Main, another CI is run which builds the
  project.
- After the build is successful, the Docker image is built and pushed to Docker Hub.
- Then, it is pushed to the Docker Swarm leader server and the Docker Swarm stack is updated.
- The leader server then updates the other nodes and deploys the latest Docker image according to
  the
  replicas.

Notes:

- Each of the master and broker projects is deployed on a specific stack.

Additional details:

- The CI/CD process is automated using GitHub Actions.
- The Docker images are built using Docker Compose.
- The Docker Swarm stack is updated using the docker stack deploy command.
- The replicas are defined in the docker-compose.yml file.

## Usage

Once installed, you can use the provided APIs to send and receive messages between services.
provided APIs are push/pull and subscribe which provide you with below functionalities:

- **Publishing messages**: use the push API to publish a message with a provided key to the message
  broker.
- **Consuming messages**: there are two ways to consume messages from broker. pulling API and the
  subscription mode.
    - clients can subscribe to the broker so that each message in message queues will be pushed to
      the clients as a server-sent event.
    - you can use the pull API to pull single message from the queue.

## Contributing

Contributions are welcome! If you’d like to enhance the broker, fix bugs, or add features, please
follow our contribution guidelines.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contact

For any questions or issues, feel free to contact us at sajad.soltaniant@gmail.com.