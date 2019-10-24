import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author platina
 * @category 精通工具类--正则表达式抽取年份
 * @time 2019年4月11日 15:40:20
 */
public class YearRegex {
    private static String moduleType = ".* \\\\(([1-9][0-9][0-9][0-9])\\\\).*";

    public static void main(String[] args) {
        System.out.println(yearReg("GoldenEye(1995)"));

    }
    public static int yearReg(String str){
        int retYear = 1994;
        Pattern patternType = Pattern.compile(moduleType);
        Matcher matcherType = patternType.matcher(str);
        while (matcherType.find()) {
            retYear = Integer.parseInt(matcherType.group(1));
        }
        return retYear;
    }
}