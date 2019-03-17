package com.elefana.perf;

import com.elefana.util.IndexUtils;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Scanner;

public class PsqlEscapePerformanceTest {
	@State(Scope.Thread)
	public static class TestState {
		final String json = readJson();

		private String readJson() {
			final StringBuilder result = new StringBuilder();
			final Scanner scanner = new Scanner(JsonFlattenPerformanceTest.TestState.class.getResourceAsStream("/complex.json"));
			while(scanner.hasNext()) {
				result.append(scanner.next());
			}
			scanner.close();
			return result.toString();
		}
	}

	@Benchmark
	@BenchmarkMode(value= Mode.Throughput)
	@Group("JSON")
	public void testFlatten(JsonFlattenPerformanceTest.TestState state) throws IOException {
		final String result = IndexUtils.psqlEscapeString(state.json);
	}
}
