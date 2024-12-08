package org.example.FileSystem;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class deleteHyDFSFiles {
    public static void deleteFiles() {
        String folderPath = "HyDFS"; // Replace with your folder path
        try {
            deleteFilesInFolder(folderPath);
            System.out.println("All files in the folder have been removed.");
        } catch (IOException e) {
            System.err.println("Error occurred while deleting files: " + e.getMessage());
        }
    }

    public static void deleteFilesInFolder(String folderPath) throws IOException {
        Path folder = Paths.get(folderPath);

        // Check if the folder exists
        if (!Files.exists(folder) || !Files.isDirectory(folder)) {
            throw new IllegalArgumentException("The specified path is not a valid folder: " + folderPath);
        }

        // Traverse the directory and delete files
        Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file); // Delete each file
                System.out.println("Deleted file: " + file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                System.err.println("Failed to delete file: " + file + ", " + exc.getMessage());
                return FileVisitResult.CONTINUE; // Continue even if deletion fails for a file
            }
        });
    }
}
