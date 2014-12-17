package succinct.regex.parser;

public class RegExPrimitive extends RegEx {

    private String mgram;

    public RegExPrimitive(String mgram) {
        super(RegExType.Primitive);
        this.mgram = mgram;
    }

    public String getMgram() {
        return mgram;
    }

}
