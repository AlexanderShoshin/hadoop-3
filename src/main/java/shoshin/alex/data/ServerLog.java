package shoshin.alex.data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerLog {
    private static String LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    private static int NUM_FIELDS = 9;
    private Matcher matcher;
    
    private ServerLog(Matcher matcher) {
        this.matcher = matcher;
    }
    
    public static ServerLog parse(String str) throws IllegalArgumentException {
        Pattern pattern = Pattern.compile(LOG_ENTRY_PATTERN);
        final Matcher matcher = pattern.matcher(str);
        
        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            throw new IllegalArgumentException("bad log format");
        }
        
        return new ServerLog(matcher);
    }
    
    public String getId() {
        return matcher.group(1);
    }

    public String getDateTime() {
        return matcher.group(4);
    }

    public String getRequest() {
        return matcher.group(5);
    }

    public int getResponseStatus() {
        return Integer.parseInt(matcher.group(6));
    }

    public int getSentBytes() {
        return Integer.parseInt(matcher.group(7));
    }

    public String getReferer() {
        return matcher.group(8);
    }

    public String getBrowser() {
        return matcher.group(9);
    }
}