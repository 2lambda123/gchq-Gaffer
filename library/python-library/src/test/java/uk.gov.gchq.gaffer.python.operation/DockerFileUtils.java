/*
 * Copyright 2019 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.python.operation;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class DockerFileUtils {

    private DockerFileUtils() {
        // Private constructor to hide default public one
    }

    private static void createDirectory(String directoryPath) {
        final File directory = new File(directoryPath);

        if (!directory.exists()) {
            directory.mkdir();
        }
    }

    public static Path getPathAbsolutePythonRepo(String directoryPath, String repoName) {
        BuildImageFromDockerfile bIFD = new BuildImageFromDockerfile();

        final Path pathAbsolutePythonRepo = Paths.get(directoryPath, repoName);
        return pathAbsolutePythonRepo;
    }

    public static BuildImageFromDockerfile buildDockerFile(Path pathAbsolutePythonRepo) {
        BuildImageFromDockerfile bIFD = new BuildImageFromDockerfile();

        bIFD.buildFiles(pathAbsolutePythonRepo.toString());

        return bIFD;
    }

    public static BuildImageFromDockerfile getDockerFileImage(String directoryPath, String repoName) {
        BuildImageFromDockerfile bIFD = new BuildImageFromDockerfile();

        createDirectory(directoryPath);
        Path pathAbsolutePythonRepo = getPathAbsolutePythonRepo(directoryPath, repoName);

        bIFD.buildFiles(pathAbsolutePythonRepo.toString());

        return bIFD;
    }
}
