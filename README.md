# Flink Streaming Test Tool

This project provides tooling to test Apache Flink streaming data flows. The `StreamTestEnvironment` executes the data flow and feeds a verifier with the output. The tool ships with several implementations of this verifier. 

Features include:
- Concise DSL to define test scenarios.
- Powerful matchers to express expectations.
- Test windowing with timestamped input.
- Test base for `JUnit`.

## Simple Example:
```java
class Test extends StreamTestBase {
    
    @org.junit.Test
    public myTest() {
		DataStream<Integer> stream = createTestStream(asList(1,2,3))
		    .map((MapFunction<Integer,Integer>) (value) -> {return value + 1});

		ExpectedOutput<Integer> expectedOutput = 
		    new ExpectedOutput<Integer>().expectAll(asList(2,3,4))

		assertStream(stream, expectedOutput);
    }

}
```
You can find more extensive examples here: .

## Getting started:

### Maven Central
This project is not released yet.

### Manual Build
1. Clone this repo: `git clone http://repo`.
2. Build with maven: `maven install`.
3. Include in your projects pom.xml: 
    ```xml
    <dependency>
        <groupId>org.apache.flink</groupId>
        <articaftId>flink-streaming-test</artifactId>
        <version>0.1-SNAPSHOT</version>
    <\dependency>
    ```

## Origins
The project was conceived at the Business Intelligence department of Otto Group
