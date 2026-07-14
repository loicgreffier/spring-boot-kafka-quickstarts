# Agent Instructions

## Repository Structure

- `kafka-consumer-quickstarts`: Kafka consumer code samples.
- `kafka-producer-quickstarts`: Kafka producer code samples.
- `kafka-streams-quickstarts`: Kafka Streams code samples.

Each quickstart is an independent Spring Boot module. 
A module typically contains an `app` (runner/topology), `config`, `property`, `constant`, and `serdes` package under `io.github.loicgreffier`.

## Commands

- Build with `mvn clean package`.
- Run the tests with `mvn test`.
- Run `mvn spotless:apply` before committing to apply Palantir Java Format.

## Coding Standards

- Target Java 25.
- Never use `var`. Always declare variables with their explicit type.
- Prefer guard clauses (early returns) over `if ... else ...`.
- Code follows Palantir Java Format.
- Add minimal Javadoc to every method in production code (`src/main`), including `@Override` methods, with descriptions for parameters and return values. Start each description with an uppercase letter.

## Testing Standards

- Name test classes `<ClassName>Test`.
- Name unit tests with the "should..." convention (e.g. `shouldConsumeSuccessfully`).
- Use JUnit Jupiter assertions (`org.junit.jupiter.api.Assertions`).
- Use Mockito with `@ExtendWith(MockitoExtension.class)`.
- Test Kafka Streams topologies with `TopologyTestDriver`, and test consumers/producers with Mockito's `MockConsumer`/`MockProducer`.