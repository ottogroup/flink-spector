# Flinkspector

This project provides a framework to define unit tests for Apache Flink data flows.
The framework executes data flows locally and verifies the output using predefined expectations. 

Features include:
- Concise DSL to define test scenarios.
- Powerful matchers to express expectations.
- Test base for JUnit.
- Test stream windowing with timestamped input.

Check out the [**wiki**](https://github.com/ottogroup/flink-spector/wiki) to learn how Flinkspector can assist you in developing Flink jobs.

## Examples

###Minimal:
```java
class Test extends DataSetTestBase {
    
    @org.junit.Test
    public myTest() {
		DataSet<Integer> dataSet = createTestDataSet(asList(1,2,3))
		    .map((MapFunction<Integer,Integer>) (value) -> {return value + 1});

		ExpectedRecords<Integer> expected = 
		    new ExpectedRecords<Integer>().expectAll(asList(2,3,4))

		assertDataSet(dataSet, expected);
    }

}
```

###Streaming: 
```java
@org.junit.Test
public void testWindowing() {

	// Define the input DataStream:	
	DataStream<Tuple2<Integer, String>> testStream =
			createTimedTestStreamWith(Tuple2.of(1, "fritz"))
					.emit(Tuple2.of(1, "hans"), after(15, seconds))	
					.emit(Tuple2.of(1, "heidi"), before(5, seconds))	
					.emit(Tuple2.of(3, "peter"), after(20, seconds), times(10))	
					.repeatAll(after(10, seconds), times(1))
					.close();

		
	// Lets you query the output tuples like a table:
	OutputMatcher<Tuple2<Integer, String>> matcher =
			//define keys for the values in your tuple:
			new MatchTuples<Tuple2<Integer, String>>("value", "name")
					.assertThat("value", is(3))
					.assertThat("name", either(is("fritz")).or(is("peter")))
					.onEachRecord();
	
	assertStream(someWindowAggregation(testStream), matcher);
}
```

You can find more extensive examples here: 
* [DataSet API test examples](flinkspector-dataset/src/test/java/org/flinkspector/dataset/examples).
* [DataStream API test examples](flinkspector-datastream/src/test/java/org/flinkspector/datastream/examples).

## Getting started

### Manual Build:
1. Clone this repo: `git clone https://github.com/ottogroup/flink-spector`.

> Note: The current build works with Flink version 1.0.0.
> If you're using an older version, clone the matching branch.

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

## Build Status

[![Build Status](https://travis-ci.org/ottogroup/flink-spector.svg?branch=master)](https://travis-ci.org/ottogroup/flink-spector)

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/schedoscope/blob/master/LICENSE)
