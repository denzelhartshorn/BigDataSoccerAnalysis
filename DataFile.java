package datamapreduce;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.StringTokenizer;

public class DataFile {
	private String rawText, cleanedText;

	public DataFile(String rawText) {
		this.rawText = rawText;

		this.cleanedText = CleanText(this.rawText);
	}

	private String CleanText(String rawText) {
		String cleanedText = "";
		//TODO
		cleanedText = rawText;

		return cleanedText;
	}

	public String GetCleanedText() {
		return cleanedText;
	}
}