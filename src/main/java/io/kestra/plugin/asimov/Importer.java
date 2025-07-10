package io.kestra.plugin.asimov;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "ASIMOV Importer",
    description = "Run an ASIMOV importer module with a URL and return the raw JSON-LD output."
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Import using ASIMOV DuckDuckGo module",
            code = {
                "module: \"serpapi\"",
                "url: \"https://duckduckgo.com/?q=Isaac+Asimov\""
            }
        )
    }
)
public class Importer extends Task implements RunnableTask<Importer.Output> {
    @Schema(
        title = "ASIMOV module name",
        description = "The name of the ASIMOV module to run (e.g., `serpapi`)."
    )
    private Property<String> module;

    @Schema(
        title = "URL",
        description = "The input URL to be passed to the ASIMOV module."
    )
    private Property<String> url;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String renderedModule = runContext.render(this.module).as(String.class).orElseThrow();
        String renderedUrl = runContext.render(this.url).as(String.class).orElseThrow();

        logger.info("Running ASIMOV importer module '{}' with input '{}'", renderedModule, renderedUrl);

        Process process = new ProcessBuilder("asimov-" + renderedModule + "-importer", renderedUrl)
            .redirectErrorStream(true)
            .start();

        StringBuilder outputBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logger.debug(line);
                outputBuilder.append(line).append("\n");
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("ASIMOV importer failed with exit code: " + exitCode + ". Output: " + outputBuilder.toString().trim());
        }

        return Output.builder()
            .output(outputBuilder.toString().trim())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "JSON-LD output",
            description = "The raw JSON-LD output returned by the ASIMOV module."
        )
        private final String output;
    }
}
