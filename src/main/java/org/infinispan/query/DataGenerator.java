package org.infinispan.query;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class DataGenerator {
   private static final String WORDS = "words.txt";
   private final List<String> wordList;
   private final List<String> usedWords = new ArrayList<>();
   private final Random rand = new Random(0);

   DataGenerator() {
      try {
         URL resource = this.getClass().getClassLoader().getResource(WORDS);
         if (resource == null) throw new RuntimeException("Can't find words file");
         this.wordList = Files.readAllLines(Paths.get(resource.toURI()), StandardCharsets.UTF_8);
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   String randomWord(boolean track) {
      String w = wordList.get(rand.nextInt(wordList.size()));
      if (track) {
         synchronized (this) {
            usedWords.add(w);
         }
      }
      return w;
   }

   String getUsedWord() {
      synchronized (this) {
         return usedWords.get(rand.nextInt(usedWords.size()));
      }
   }

   String randomPhrase(int size) {
      return IntStream.range(0, size).boxed().map(i -> randomWord(true)).collect(Collectors.joining(" "));
   }

   String randomPhraseWithoutTracking() {
      return IntStream.range(0, 10).boxed().map(i -> randomWord(false)).collect(Collectors.joining(" "));
   }

}
