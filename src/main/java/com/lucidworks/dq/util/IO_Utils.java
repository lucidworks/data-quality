package com.lucidworks.dq.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;


public class IO_Utils {
  
  public static File materializeSolrHomeIntoTemp() throws IOException, URISyntaxException {
    String prefix = "solr_dq_utils_";
    String topName = "solr_home";
    //String magicName = "configsets";
    Path baseTempDir = Files.createTempDirectory( prefix );
    // File destinationDir = new File( baseTempDir.toFile(), magicName );
    File destinationDir = new File( baseTempDir.toFile(), topName );
    if ( ! destinationDir.mkdirs() ) {
      throw new IOException( "Unable to create path \"" + destinationDir + "\"" );
    }
    // System.out.println( "Created \"" + destinationDir + "\"" );
    IO_Utils iou = new IO_Utils();
  
    //String sourcePathWithinJar = "/";
    // ^-- gets all classes from every combined jar
    
    //String sourcePathWithinJar = "configsets";
    // ^-- Exception in thread "main" java.lang.IllegalArgumentException, no details
    
    // String sourcePathWithinJar = "/" + magicName;
    String sourcePathWithinJar = "/" + topName;
  
    // String destinationPathInFilesystem = "/Users/mbennett/tmp_test_copy";
    // ^-- Doesn't create spanning .../configsets/... dir, just subdirectories of it

    // iou.copyFromJar( sourcePathWithinJar, Paths.get(destinationPathInFilesystem) );
    iou.copyFromJar( sourcePathWithinJar, Paths.get(destinationDir.toString()) );
    return destinationDir;
  }

  // Parts take from:
  // * http://stackoverflow.com/a/24316335/295802
  // * http://codingjunkie.net/java-7-copy-move/
  // Usage: copyFromJar("/path/to/the/template/in/jar", Paths.get("/tmp/from-jar"))
  public void copyFromJar(String source, final Path target) throws URISyntaxException, IOException {
    System.out.println( "source str = \"" + source + "\"" );

    
    // getClass is defined in Object
    URI resource = getClass().getResource("").toURI();

    // ... ? FileSystems.newFileSystem(...)
    // ^-- java.lang.IllegalArgumentException: Path component should be '/'
    //     at least when run in Eclipse (non .jar packaging)
    //URI resource = getClass().getResource("/").toURI();

    System.out.println( "URI Resource = \"" + resource + "\"" );
    // ^-- Interactive: "file:/Users/mbennett/data/dev/DQ/data-quality-github/target/classes/"
    // ^-- Run Uberjar: "jar:file:/Users/mbennett/data/dev/DQ/data-quality-github/target/data-quality-java-1.0-SNAPSHOT.jar!/com/lucidworks/dq/util/"

    // jar:file: - Running from packaged jar
    if ( resource.toString().startsWith("jar:file:" ) ) {
      FileSystem fileSystem = FileSystems.newFileSystem(
          resource,
          Collections.<String, String>emptyMap()
          );
  
      final Path jarPath = fileSystem.getPath(source);
  
      // Recursive copy
      // TODO: looks similar to other recursive copy below, maybe combine
      Files.walkFileTree(jarPath, new SimpleFileVisitor<Path>() {
  
        private Path currentTarget;
  
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          currentTarget = target.resolve(jarPath.relativize(dir).toString());
          Files.createDirectories(currentTarget);
          return FileVisitResult.CONTINUE;
        }
  
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          //System.out.println( "Copying \"" + file.toString() + "\" ..." );
          Files.copy(file, target.resolve(jarPath.relativize(file).toString()), StandardCopyOption.REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }
  
      });
    
    }
    // file: - Running from Eclipse or other non-packaged runner
    else if ( resource.toString().startsWith("file:" ) ) {
      // Our resource is relative root level, not this specific package
      URI resource2 = getClass().getResource("/").toURI();
      File base = new File( resource2.getPath() );
      File srcDir = new File( base, source );
      final Path fromPath = srcDir.toPath();
      final Path toPath = target;

      // Recursive copy
      // TODO: looks similar to other recursive copy above, maybe combine
      Files.walkFileTree(fromPath, new SimpleFileVisitor<Path>() {

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          Path targetPath = toPath.resolve(fromPath.relativize(dir));
          if ( ! Files.exists(targetPath) ){
            Files.createDirectory(targetPath);
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.copy(file, toPath.resolve(fromPath.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }

      });
      
      
      /***
      // TODO: recursive copy from filesystem
      // Files.copy( new File(source).toPath(), target, StandardCopyOption.REPLACE_EXISTING );
      // ^-- No, only has "/solr_home"
      // and "resource" is too far down:
      // Gives: /Users/mbennett/data/dev/DQ/data-quality-github/target/classes/com/lucidworks/dq/util
      //  Need: /Users/mbennett/data/dev/DQ/data-quality-github/target/classes/solr_home
      URI resource2 = getClass().getResource("/").toURI();
      // gives! file:/Users/mbennett/data/dev/DQ/data-quality-github/target/classes/
      System.out.println( "URI Resource2 = \"" + resource2 + "\"" );
      File base = new File( resource2.getPath() );
      File srcDir = new File( base, source );
      Path srcPath = srcDir.toPath();
      System.out.println( "srcPath = \"" + srcPath + "\"" );
      System.out.println( "target = \"" + target + "\"" );
      //Files.copy( srcPath, target, StandardCopyOption.REPLACE_EXISTING );

      // EnumSet<FileVisitOption> opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
      // TreeCopier tc = new TreeCopier(source[i], dest, prompt, preserve);
      // Files.walkFileTree(source[i], opts, Integer.MAX_VALUE, tc);

      ***/
    
    
    }
    else {
      throw new IllegalArgumentException( "Don't know how to handle " + resource );
    }

  }


  
  
  public static void main(String[] args) throws URISyntaxException, IOException {
    //File configSetsDir = materializeConfigsetsInTemp();
    File configSetsDir = materializeSolrHomeIntoTemp();
    System.out.println( "ConfigSets = " + configSetsDir );
    
  }
}
