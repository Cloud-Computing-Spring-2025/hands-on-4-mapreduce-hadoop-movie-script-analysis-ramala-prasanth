

# Movie Script Analysis using Hadoop MapReduce

## Project Overview
This project implements a Hadoop MapReduce program that analyzes movie script dialogues to extract insights such as the most frequently spoken words by characters, total dialogue length per character, and the unique words used by each character. The project also integrates Hadoop Counters to track key statistics during the MapReduce job execution. The input dataset consists of dialogues from movie scripts, where each line contains a character's name followed by their dialogue. The output is a structured summary of the most frequent words, dialogue lengths, and unique words used by each character, along with Hadoop Counters detailing the number of lines, words, characters, unique words, and speaking characters.

## Approach and Implementation

### Mapper:
The Mapper processes each line of the input data, extracting the character's name and their spoken dialogue. For each line, the Mapper:
1. **Extracts the character name** and the dialogue text.
2. **Tokenizes the dialogue** into individual words, and performs basic cleaning (e.g., removing punctuation).
3. **Generates multiple outputs**:
   - A word count for each word in the dialogue (word -> 1).
   - A dialogue length count (character -> length of dialogue).
   - A set of unique words for each character.
4. **Updates Hadoop Counters**:
   - Count of lines processed.
   - Count of words processed.
   - Count of characters processed.
   - Count of unique words identified.
   - Count of unique characters speaking.

### Reducer:
The Reducer aggregates the outputs from the Mapper. For each character, the Reducer:
1. **Calculates the most frequently spoken words** by summing the counts from the Mapper.
2. **Summarizes the total dialogue length** by summing the lengths for each character.
3. **Extracts the unique words used by each character** by combining all word lists.
4. **Generates output data for the most frequent words, dialogue lengths, and unique words used**.

### Hadoop Counters:
The program tracks the following counters:
1. **Total Lines Processed** – Total number of lines (dialogues) in the input dataset.
2. **Total Words Processed** – Total number of words processed across all dialogues.
3. **Total Characters Processed** – Total number of characters processed across all dialogues.
4. **Total Unique Words Identified** – Total number of unique words used across the entire dataset.
5. **Number of Characters Speaking Dialogues** – The number of unique characters who speak in the script.

## Execution Steps

### Step 1: Set up the Maven Project
1. Clone the repository and navigate to the project directory.
2. Run `mvn clean install` to build the project.

### Step 2: Prepare Input Dataset
1. Place the movie script text file in the input directory. Example file format:
   ```
   JACK: The ship is sinking! We have to go now.
   ROSE: I won’t leave without you.
   JACK: We don’t have time, Rose!
   ```

### Step 3: Run the MapReduce Job
To run the Hadoop job:
1. Start Hadoop if it's not already running.
2. Use the following command to run the MapReduce job:

   ```bash
   hadoop jar movie-script-analysis.jar com.example.MovieScriptAnalysis /input/dataset /output
   ```

### Step 4: View the Output
1. The output for tasks 1, 2, and 3 will be stored in the `/output` directory.
2. The Hadoop counter results (Task 4) will be displayed in the terminal output after job execution.

### Step 5: Commit the Output to GitHub
1. Copy the output files from the Hadoop output directory.
2. Commit them to your GitHub repository.

## Output Details

### 1. Most Frequently Spoken Words by Characters
The output for most frequent words is displayed in the following format, showing the word and its frequency count for each character:
```
the 3
we 3
have 2
to 2
now 1
without 1
```

### 2. Total Dialogue Length per Character
The output for total dialogue length per character is shown as:
```
JACK 54
ROSE 25
```
This shows the sum of the character count of all dialogues spoken by each character.

### 3. Unique Words Used by Each Character
The output for unique words used by each character is shown as:
```
JACK [the, ship, is, sinking, we, have, to, go, now, don’t, time, rose]
ROSE [i, won’t, leave, without, you]
```
This displays the list of unique words used by each character in the script.

### 4. Hadoop Counter Output
The Hadoop counters that track the key statistics are displayed in the terminal output after the MapReduce job finishes. The following statistics will be printed:

```
Total Lines Processed: 3
Total Words Processed: 18
Total Characters Processed: 79
Total Unique Words Identified: 13
Number of Characters Speaking: 2
```

## Challenges Faced & Solutions
- **Challenge:** Parsing the movie script dialogues and handling punctuation and special characters.
  - **Solution:** Used regular expressions to clean the dialogues by removing punctuation marks and converting all words to lowercase.
- **Challenge:** Correctly tracking unique words and handling case-sensitivity.
  - **Solution:** Used a `HashSet` in the Mapper to track unique words for each character, which ensures that duplicates are eliminated.

## Sample Input and Output

### Sample Input:
```
JACK: The ship is sinking! We have to go now.
ROSE: I won’t leave without you.
JACK: We don’t have time, Rose!
```

### Expected Output:

#### 1. Most Frequently Spoken Words by Characters:
```
the 3
we 3
have 2
to 2
now 1
without 1
```

#### 2. Total Dialogue Length per Character:
```
JACK 54
ROSE 25
```

#### 3. Unique Words Used by Each Character:
```
JACK [the, ship, is, sinking, we, have, to, go, now, don’t, time, rose]
ROSE [i, won’t, leave, without, you]
```

#### 4. Hadoop Counter Output:
```
Total Lines Processed: 3
Total Words Processed: 18
Total Characters Processed: 79
Total Unique Words Identified: 13
Number of Characters Speaking: 2
```

---



