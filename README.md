# Flinkspector

This project provides a framework to define unit tests for Apache Flink data flows.
The framework executes the data flows locally and verifies the output using predefined expectations. 

Features include:
- Concise DSL to define test scenarios.
- Powerful matchers to express expectations.
- Test base for `JUnit`.
- Test stream windowing with timestamped input.

## Minimal Example
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
You can find more extensive examples here: 
* [DataSet API test examples](flinkspector-dataset/src/test/java/org/flinkspector/dataset/examples).
* [DataStream API test examples](flinkspector-datastream/src/test/java/org/flinkspector/datastream/examples).

## Getting started

### Manual Build:
1. Clone this repo: `git clone https://github.com/ottogroup/flink-spector`.
2. Build with maven: `maven install`.
3. Include in your project's pom.xml: 
```xml
<dependency>
    <groupId>org.flinkspector</groupId>
    <articaftId>flinkspector-dataset</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```
or for the Flink DataStream API:
    
```xml
<dependency>
    <groupId>org.flinkspector</groupId>
    <articaftId>flinkspector-datastream</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```

## Origins
The project was conceived at the Business Intelligence department of Otto Group.
