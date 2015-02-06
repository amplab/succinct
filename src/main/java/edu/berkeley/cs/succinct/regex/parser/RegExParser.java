package edu.berkeley.cs.succinct.regex.parser;

public class RegExParser {

    private String exp;
    private static final RegEx BLANK = new RegExBlank();

    public RegExParser(String exp) {
        this.exp = exp;
    }

    public RegEx parse() throws RegExParsingException {
        return regex();
    }

    private char peek() {
        return exp.charAt(0);
    }

    private void eat(char c) throws RegExParsingException {
        if(peek() == c) {
            exp = exp.substring(1);
        } else {
            String message = "Could not parse regex expression; peek() = " + peek() + " trying to eat() = " + c;
            throw new RegExParsingException(message);
        }
    }

    private char next() throws RegExParsingException {
        char c = peek();
        eat(c);
        return c;
    }

    private char nextChar() throws RegExParsingException {
        if(peek() == '\\') {
            eat('\\');
        }
        return next();
    }

    private int nextInt() throws RegExParsingException {
        int num = 0;
        while(peek() >= 48 && peek() <= 57) {
            num = num * 10 + (next() - 48);
        }
        return num;
    }

    private boolean more() {
        return (exp.length() > 0);
    }

    private RegEx regex() throws RegExParsingException {
        RegEx t = term();
        if(more() && peek() == '|') {
            eat('|');
            RegEx r  = regex();
            return new RegExUnion(t, r);
        }
        return t;
    }

    private RegEx concat(RegEx a, RegEx b) {
        if(a.getRegExType() == RegExType.Blank) {
            return b;
        } else if(a.getRegExType() == RegExType.Primitive && b.getRegExType() == RegExType.Primitive) {
            String aStr = ((RegExPrimitive)a).getMgram();
            String bStr = ((RegExPrimitive)b).getMgram();
            return new RegExPrimitive(aStr + bStr);
        }
        return new RegExConcat(a, b);
    }

    private RegEx term() throws RegExParsingException {
        RegEx f = BLANK;
        while(more() && peek() != ')' && peek() != '|') {
            RegEx nextF = factor();
            f = concat(f, nextF);
        }
        return f;
    }

    private RegEx factor() throws RegExParsingException {
        RegEx b = base();

        if(more() && peek() == '*') {
            eat('*');
            b = new RegExRepeat(b, RegExRepeatType.ZeroOrMore);
        } else if(more() && peek() == '+') {
            eat('+');
            b = new RegExRepeat(b, RegExRepeatType.OneOrMore);
        } else if(more() && peek() == '{') {
            eat('{');
            int min = nextInt();
            eat(',');
            int max = nextInt();
            eat('}');
            b = new RegExRepeat(b, RegExRepeatType.MinToMax, min, max);
        }

        return b;
    }

    private RegEx base() throws RegExParsingException {
        if(peek() == '(') {
            eat('(');
            RegEx r = regex();
            eat(')');
            return r;
        }
        return mgram();
    }

    private RegEx mgram() throws RegExParsingException {
        String m = "";
        while(more() && peek() != '|' && peek() != '(' && peek() != ')' && peek() != '*' && peek() != '+' &&
                peek() != '{' && peek() != '}') {
            m += nextChar();
        }
        return new RegExPrimitive(m);
    }

}
