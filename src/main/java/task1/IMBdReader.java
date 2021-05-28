package task1;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

import java.time.Year;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class IMBdReader {
    private final static String BASIC_FILE_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/input/title.basics.tsv";
    private final static String RATING_FILE_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/input/title.ratings.tsv";
    private final static String NAME_BASICS_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/input/name.basics.tsv";
    private final static String PRINCIPALS_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/input/title.principals.tsv";
    private final static String TOP_100_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_100.csv";
    private final static String TOP_100_FOR_LAST_10_YEARS_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_100_for_last_10_years.csv";
    private final static String TOP_100_FOR_LAST_FOR_196X_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_100_for_last_196x.csv";
    private final static String TOP_10_GENRE_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_10_genre.csv";
    private final static String TOP_10_GENRE_BY_DECADE_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_10_genre_by_decade.csv";
    private final static String TOP_ACTORS_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_actors.csv";
    private final static String TOP_FILMS_FOR_DIRECTORS_OUTPUT_PATH = "/Users/alyonashapoval/Documents/projects/shapoval_spark" +
            "/src/main/resources/task1/output/result_top_films_for_directors.csv";
    private static List<DecadeDateValues> decadeDateValues = null;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Shapoval training")
                .getOrCreate();
        spark.sparkContext().setLogLevel("INFO");
        Dataset<Row> dfRating = spark.read()
                .option("delimiter", "\t")
                .option("header", "true")
                .csv(RATING_FILE_PATH)
                .filter(col("numVotes").$greater$eq(100000))
                .cache();
        Dataset<Row> dfBasics = spark.read()
                .option("delimiter", "\t")
                .option("header", "true")
                .csv(BASIC_FILE_PATH)
                .cache();
        Dataset<Row> dfNameBasics = spark.read()
                .option("delimiter", "\t")
                .option("header", "true")
                .csv(NAME_BASICS_PATH)
                .cache();
        Dataset<Row> dfPrincipals = spark.read()
                .option("delimiter", "\t")
                .option("header", "true")
                .csv(PRINCIPALS_PATH)
                .cache();
        Dataset<Row> dfBasicsMovies = dfBasics
                .filter(col("titleType").equalTo("movie"))
                .cache();
        Dataset<Row> dfBasicsSplittedRow = dfBasics
                .withColumn("genre", functions.explode(functions.split(col("genres"), "\\,")))
                .cache();
        Dataset<Row> dfBasicsSplittedRowAndDecades = dfBasics
                .withColumn("genre", functions.explode(functions.split(col("genres"), "\\,")))
                .withColumn("currentYear", year(functions.current_date()))
                .withColumn("yearRange", functions.concat(
                        col("currentYear"), lit("_"), col("currentYear").minus(10)))
                .cache();
        Dataset<Row> dfNameBasicsToActors = dfNameBasics
                .filter(functions.col("primaryProfession").contains("actor")
                        .or(functions.col("primaryProfession").contains("actress")))
                .cache();
        Dataset<Row> dfNameBasicsToDirectors = dfNameBasics
                .filter(functions.col("primaryProfession").contains("director"))
                .cache();

        Dataset<Row> dfPrincipalsToActors = dfPrincipals
                .filter(functions.col("category").equalTo("actor")
                        .or(functions.col("category").equalTo("actress")))
                .cache();
        Dataset<Row> dfPrincipalsToDirectors = dfPrincipals
                .filter(functions.col("category").equalTo("director"))
                .cache();
        decadeDateValues = fillInDecadeValues();
        spark.udf().register("findYearRange", findYearRange, DataTypes.StringType);
        //top 100 movies
        Dataset<Row> joinedTitles = selectTopFilms(dfBasicsMovies, dfRating);
        //top 100 movies for last 10 years
        Dataset<Row> joinedTitlesLastTenYears = selectTop100FilmsForLast10Years(dfBasicsMovies, dfRating);
        //top 100 movies for 1960x
        Dataset<Row> joinedTitlesTheSixtesYears = selectTop100FilmsForLast60s(dfBasicsMovies, dfRating);
        //top 10 movies for each genre
        Dataset<Row> joinedTitlesTopTenInGenre = selectTop10FilmsByGenre(dfBasicsSplittedRow, dfRating);
        //top 10 movies for each genre by decade
        Dataset<Row> joinedTitlesTopTenInGenreByDecade = selectTop10ByGenreForEachDecade(dfBasicsSplittedRowAndDecades, dfRating);
        //top actors
        Dataset<Row> joinedTopActors = selectTheMostPopularActors(dfNameBasicsToActors,
                selectTopFilms(dfBasicsMovies, dfRating), dfPrincipalsToActors);
        // top films by director
        Dataset<Row> joinedTopFilmsForDirectors = selectTop5FilmsForDirectors(dfNameBasicsToDirectors,
                selectTopFilms(dfBasicsMovies, dfRating), dfPrincipalsToDirectors);

        writeToFile(TOP_100_OUTPUT_PATH, joinedTitles);
        writeToFile(TOP_100_FOR_LAST_10_YEARS_OUTPUT_PATH, joinedTitlesLastTenYears);
        writeToFile(TOP_100_FOR_LAST_FOR_196X_OUTPUT_PATH, joinedTitlesTheSixtesYears);
        writeToFile(TOP_10_GENRE_OUTPUT_PATH, joinedTitlesTopTenInGenre);
        writeToFile(TOP_10_GENRE_BY_DECADE_OUTPUT_PATH, joinedTitlesTopTenInGenreByDecade);
        writeToFile(TOP_ACTORS_OUTPUT_PATH, joinedTopActors);
        writeToFile(TOP_FILMS_FOR_DIRECTORS_OUTPUT_PATH, joinedTopFilmsForDirectors);


        spark.close();


    }

    private static void writeToFile(String filePath, Dataset<Row> df) {
        df.write()
                .option("delimiter", ",")
                .mode(SaveMode.Overwrite)
                .csv(filePath);
    }

    private static Dataset<Row> selectTopFilms(Dataset<Row> dfBasicsMovies, Dataset<Row> dfRating) {
        return dfBasicsMovies
                .join(dfRating, dfBasicsMovies.col("tconst").equalTo(dfRating.col("tconst")))
                .drop(dfBasicsMovies.col("tconst"))
                .orderBy(dfRating.col("averageRating").desc())
                .select("tconst", "primaryTitle", "numVotes", "averageRating", "startYear")
                .cache();
    }

    private static Dataset<Row> selectTop100FilmsForLast10Years(Dataset<Row> dfBasicsMovies, Dataset<Row> dfRating) {
        Row maxDate = dfBasicsMovies
                .filter(col("startYear").notEqual("\\N"))
                .sort(dfBasicsMovies.col("startYear").desc())
                .first();
        return dfBasicsMovies
                .join(dfRating, dfBasicsMovies.col("tconst").equalTo(dfRating.col("tconst")))
                .drop(dfBasicsMovies.col("tconst"))
                .orderBy(dfRating.col("averageRating").desc())
                .filter(year(lit(maxDate.getAs("startYear")))
                        .$minus(year(dfBasicsMovies.col("startYear"))).$less$eq(10))
                .limit(100)
                .select("tconst", "primaryTitle", "titleType", "numVotes", "averageRating", "startYear")
                .cache();
    }

    private static Dataset<Row> selectTop100FilmsForLast60s(Dataset<Row> dfBasicsMovies, Dataset<Row> dfRating) {
        return dfBasicsMovies
                .join(dfRating, dfBasicsMovies.col("tconst").equalTo(dfRating.col("tconst")))
                .drop(dfBasicsMovies.col("tconst"))
                .filter(dfBasicsMovies.col("startYear").like("196%"))
                .orderBy(dfRating.col("averageRating").desc())
                .limit(100)
                .select("tconst", "primaryTitle", "numVotes", "averageRating", "startYear")
                .cache();
    }

    private static Dataset<Row> selectTop10FilmsByGenre(Dataset<Row> dfBasicsSplittedRow, Dataset<Row> dfRating) {
        return dfBasicsSplittedRow
                .join(dfRating, dfBasicsSplittedRow.col("tconst").equalTo(dfRating.col("tconst")))
                .drop(dfBasicsSplittedRow.col("tconst"))
                .withColumn("partitionId", functions.row_number().over(
                        Window.partitionBy("genre").orderBy(dfRating.col("averageRating").desc())))
                .filter(col("partitionId").$less$eq(10))
                .select("partitionId", "tconst", "primaryTitle", "startYear", "genre", "averageRating", "numVotes")
                .cache();
    }

    private static Dataset<Row> selectTop10ByGenreForEachDecade(Dataset<Row> dfBasicsSplittedRowAndDecades, Dataset<Row> dfRating) {

        Dataset<Row> filteredBasics = dfBasicsSplittedRowAndDecades
                .filter(col("startYear").notEqual("\\N").and(col("startYear").like("195%")))
                .cache();
        return filteredBasics
                .join(dfRating, filteredBasics.col("tconst").equalTo(dfRating.col("tconst")))
                .drop(filteredBasics.col("tconst"))
                .withColumn("partitionId", functions.row_number().over(
                        Window.partitionBy("genre").orderBy(dfRating.col("averageRating").desc(),
                                filteredBasics.col("startYear"))))
                .withColumn("yearRange", callUDF("findYearRange", col("startYear")))
                .filter(col("partitionId").$less$eq(10))
                .select("partitionId", "tconst", "primaryTitle", "startYear", "genre"
                        , "averageRating", "numVotes", "yearRange")
                .cache();


    }

    private static UDF1 findYearRange = new UDF1<String, String>() {
        @Override
        public String call(String year) throws Exception {
            for (DecadeDateValues value : decadeDateValues) {
                Year convertedYear = Year.parse(year);
                if (convertedYear.isAfter(value.getSecondDate()) && convertedYear.isBefore(value.getFirstDate())
                        || convertedYear.equals(value.getSecondDate()) || convertedYear.equals(value.getFirstDate())) {
                    return value.getYearRange();
                }
            }
            return null;
        }
    };

    private static List<DecadeDateValues> fillInDecadeValues() {
        Year now = Year.now();
        List<DecadeDateValues> decadeDateValues = new ArrayList<>();
        Year finalDate = Year.parse("1950");
        Year prevDate = null;
        do {
            if (prevDate == null) {
                Year secondDate = now.minusYears(10);
                decadeDateValues.add(new DecadeDateValues(now + "_" + secondDate, now, secondDate));
                prevDate = secondDate;
            }
            Year secondDate = prevDate.minusYears(10);
            decadeDateValues.add(new DecadeDateValues(prevDate + "_" + secondDate, prevDate, secondDate));
            prevDate = secondDate;
        } while (prevDate.isAfter(finalDate));
        return decadeDateValues;
    }

    private static Dataset<Row> selectTheMostPopularActors(Dataset<Row> dfNameBasicsToActors, Dataset<Row> topFilms,
                                                           Dataset<Row> principals) {
        Dataset<Row> topActors = principals
                .join(topFilms, principals.col("tconst").equalTo(topFilms.col("tconst")))
                .drop(principals.col("tconst"))
                .cache()
                .groupBy("nconst")
                .count()
                .filter(col("count").$greater$eq(10))
                .cache();

        return topActors
                .join(dfNameBasicsToActors, topActors.col("nconst").equalTo(dfNameBasicsToActors.col("nconst")))
                .drop(topActors.col("nconst"))
                .select("primaryName")
                .cache();

    }

    private static Dataset<Row> selectTop5FilmsForDirectors(Dataset<Row> dfNameBasicsToDirectors, Dataset<Row> topFilms,
                                                            Dataset<Row> principals) {
        Dataset<Row> groupedByDirectors = principals
                .join(dfNameBasicsToDirectors, principals.col("nconst").equalTo(dfNameBasicsToDirectors.col("nconst")))
                .drop(principals.col("nconst"))
                .orderBy("nconst");
        return groupedByDirectors
                .join(topFilms, topFilms.col("tconst").equalTo(groupedByDirectors.col("tconst")))
                .drop(groupedByDirectors.col("tconst"))
                .withColumn("partitionId", functions.row_number().over(
                        Window.partitionBy("nconst").orderBy(topFilms.col("averageRating").desc())))
                .filter(col("partitionId").$less$eq(10))
                .select("primaryName", "primaryTitle", "startYear", "averageRating", "numVotes");

    }
}
