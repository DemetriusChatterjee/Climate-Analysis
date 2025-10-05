/**
 * climate.c
 *
 * Performs analysis on climate data provided by the
 * National Oceanic and Atmospheric Administration (NOAA).
 *
 * Input:    Tab-delimited file(s) to analyze.
 * Output:   Summary information about the data.
 *
 * Compile:  run make
 *
 * Example Run:      ./climate data_tn.tdv data_wa.tdv
 *
 *
 * Opening file: data_tn.tdv
 * Opening file: data_wa.tdv
 * States found: TN WA
 * -- State: TN --
 * Number of Records: 17097
 * Average Humidity: 49.4%
 * Average Temperature: 58.3F
 * Max Temperature: 110.4F 
 * Max Temperatuer on: Mon Aug  3 11:00:00 2015
 * Min Temperature: -11.1F
 * Min Temperature on: Fri Feb 20 04:00:00 2015
 * Lightning Strikes: 781
 * Records with Snow Cover: 107
 * Average Cloud Cover: 53.0%
 * -- State: WA --
 * Number of Records: 48357
 * Average Humidity: 61.3%
 * Average Temperature: 52.9F
 * Max Temperature: 125.7F
 * Max Temperature on: Sun Jun 28 17:00:00 2015
 * Min Temperature: -18.7F 
 * Min Temperature on: Wed Dec 30 04:00:00 2015
 * Lightning Strikes: 1190
 * Records with Snow Cover: 1383
 * Average Cloud Cover: 54.5%
 *
 * TDV format:
 *
 * CA» 1428300000000»  9prcjqk3yc80»   93.0»   0.0»100.0»  0.0»95644.0»277.58716
 * CA» 1430308800000»  9prc9sgwvw80»   4.0»0.0»100.0»  0.0»99226.0»282.63037
 * CA» 1428559200000»  9prrremmdqxb»   61.0»   0.0»0.0»0.0»102112.0»   285.07513
 * CA» 1428192000000»  9prkzkcdypgz»   57.0»   0.0»100.0»  0.0»101765.0» 285.21332
 * CA» 1428170400000»  9prdd41tbzeb»   73.0»   0.0»22.0»   0.0»102074.0» 285.10425
 * CA» 1429768800000»  9pr60tz83r2p»   38.0»   0.0»0.0»0.0»101679.0»   283.9342
 * CA» 1428127200000»  9prj93myxe80»   98.0»   0.0»100.0»  0.0»102343.0» 285.75
 * CA» 1428408000000»  9pr49b49zs7z»   93.0»   0.0»100.0»  0.0»100645.0» 285.82413
 *
 * Each field is separated by a tab character \t and ends with a newline \n.
 *
 * Fields:
 *      state code (e.g., CA, TX, etc),
 *      timestamp (time of observation as a UNIX timestamp),
 *      geolocation (geohash string),
 *      humidity (0 - 100%),
 *      snow (1 = snow present, 0 = no snow),
 *      cloud cover (0 - 100%),
 *      lightning strikes (1 = lightning strike, 0 = no lightning),
 *      pressure (Pa),
 *      surface temperature (Kelvin)
 */

#include <float.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define NUM_STATES 50
#define LINE_BUFFER 100

struct climate_info {
    char code[3];
    unsigned long num_records;
    long double sum_temperature;
    long double sum_humidity;
    long double sum_cloudcover;
    unsigned long lightning_strikes;
    unsigned long snow_records;
    time_t max_temp_time;
    time_t min_temp_time;
    long double max_temp;
    long double min_temp;
};

int analyze_file(FILE *file, struct climate_info **states, int num_states);
void print_report(struct climate_info *states[], int num_states);

int main(int argc, char *argv[]) {
    // Check if any files were provided
    if (argc < 2) { //checks if there are enough arguments
        fprintf(stderr, "Not enough arguments provided. No file provided to analyze.\n");
        return EXIT_FAILURE;
    }

    // Print all filenames first
    int i;
    for (i = 1; i < argc; ++i) {
        printf("Opening file: %s\n", argv[i]);
    }

    struct climate_info *states[NUM_STATES] = { NULL };
    int files_processed = 0;  // Track if any files were successfully processed

    // Process the files
    for (i = 1; i < argc; ++i) {
        FILE *file = fopen(argv[i], "r");
        if (file == NULL) {
            fprintf(stderr, "Unable to open file: %s\n", argv[i]);
            continue;
        }

        if (analyze_file(file, states, NUM_STATES) == 0) {
            files_processed++;
        } else {
            fprintf(stderr, "Error processing file: %s\n", argv[i]);
        }
        fclose(file);
    }

    // Check if any files were successfully processed
    if (files_processed == 0) {
        fprintf(stderr, "No valid files were processed.\n");
        return EXIT_FAILURE;
    }

    print_report(states, NUM_STATES);

    // Free allocated memory
    for (i = 0; i < NUM_STATES; i++) {
        if (states[i] != NULL) {
            free(states[i]);
        }
    }

    return EXIT_SUCCESS;
}

// Modified analyze_file to return an error code
int analyze_file(FILE *file, struct climate_info **states, int num_states) {
    if (file == NULL || states == NULL || num_states <= 0) {
        return -1;  // Invalid parameters
    }

    char line[LINE_BUFFER];
    int lines_processed = 0;

    while (fgets(line, LINE_BUFFER, file) != NULL) {
        // Check for line length overflow
        if (strlen(line) >= LINE_BUFFER - 1) {
            //fprintf(stderr, "Warning: Skipping line due to length overflow\n");
            continue;
        }

        char state_code[3];
        unsigned long long timestamp;
        char geohash[13];
        double humidity, snow, cloudcover, lightning, pressure, temperature;
        
        // Parse line using tab as delimiter
        if (sscanf(line, "%2s\t%llu\t%12s\t%lf\t%lf\t%lf\t%lf\t%lf\t%lf",
                state_code, &timestamp, geohash, &humidity, &snow,
                &cloudcover, &lightning, &pressure, &temperature) != 9)
        {
            //fprintf(stderr, "Warning: Skipping malformed line\n");
            continue;
        }

        // Validate data ranges
        if (humidity < 0 || humidity > 100 || 
            cloudcover < 0 || cloudcover > 100 || 
            temperature < 0) {  // Kelvin can't be negative
            //fprintf(stderr, "Warning: Skipping line with invalid data ranges\n");
            continue;
        }

        // Find or create state entry
        int state_idx = -1;
        for (int i = 0; i < num_states; i++) {
            if (states[i] != NULL && strcmp(states[i]->code, state_code) == 0) {
                state_idx = i;
                break;
            } else if (states[i] == NULL) {
                state_idx = i;
                states[i] = calloc(1, sizeof(struct climate_info));
                if (states[i] == NULL) {
                    fprintf(stderr, "Error: Memory allocation failed\n");
                    return -1;
                }
                strncpy(states[i]->code, state_code, 2);
                states[i]->code[2] = '\0';
                states[i]->max_temp = -DBL_MAX;
                states[i]->min_temp = DBL_MAX;
                break;
            }
        }

        if (state_idx == -1) {
            //fprintf(stderr, "Warning: No space for new state\n");
            continue;
        }

        // Update state info
        struct climate_info *info = states[state_idx];
        info->num_records++;
        info->sum_temperature += temperature;
        info->sum_humidity += humidity;
        info->sum_cloudcover += cloudcover;
        info->lightning_strikes += (lightning > 0 ? 1 : 0);
        info->snow_records += (snow > 0 ? 1 : 0);

        if (temperature > info->max_temp) {
            info->max_temp = temperature;
            info->max_temp_time = timestamp / 1000;
        }
        if (temperature < info->min_temp) {
            info->min_temp = temperature;
            info->min_temp_time = timestamp / 1000;
        }

        lines_processed++;
    }

    return (lines_processed > 0) ? 0 : -1;  // Return success if any lines were processed
}

// method to print the summary for each state
void print_report(struct climate_info *states[], int num_states) {
    printf("States found: ");  // Changed to avoid extra newline
    int i;
    for (i = 0; i < num_states; ++i) {
        if (states[i] != NULL) {
            printf("%s ", states[i]->code);
        }
    }
    printf("\n");

    for (i = 0; i < num_states; i++) {  // Changed to reuse existing i variable
        if (states[i] == NULL) continue;

        struct climate_info *info = states[i];
        printf("-- State: %s --\n", info->code);
        printf("Number of Records: %lu\n", info->num_records);
        printf("Average Humidity: %.1Lf%%\n", info->sum_humidity / info->num_records);
        printf("Average Temperature: %.1LfF\n", 
               (info->sum_temperature / info->num_records - 273.15) * 9/5 + 32);
        printf("Max Temperature: %.1LfF\n", 
               (info->max_temp - 273.15) * 9/5 + 32);
        printf("Max Temperature on: %s", ctime(&info->max_temp_time));
        printf("Min Temperature: %.1LfF\n",
               (info->min_temp - 273.15) * 9/5 + 32);
        printf("Min Temperature on: %s", ctime(&info->min_temp_time));
        printf("Lightning Strikes: %lu\n", info->lightning_strikes);
        printf("Records with Snow Cover: %lu\n", info->snow_records);
        printf("Average Cloud Cover: %.1Lf%%\n", info->sum_cloudcover / info->num_records);
    }
}
