package task1;

import java.time.Year;

public class DecadeDateValues {
    private String yearRange;
    private Year firstDate;
    private Year secondDate;

    public DecadeDateValues(String yearRange, Year firstDate, Year secondDate) {
        this.yearRange = yearRange;
        this.firstDate = firstDate;
        this.secondDate = secondDate;
    }

    public String getYearRange() {
        return yearRange;
    }

    public void setYearRange(String yearRange) {
        this.yearRange = yearRange;
    }

    public Year getFirstDate() {
        return firstDate;
    }

    public void setFirstDate(Year firstDate) {
        this.firstDate = firstDate;
    }

    public Year getSecondDate() {
        return secondDate;
    }

    public void setSecondDate(Year secondDate) {
        this.secondDate = secondDate;
    }
}
