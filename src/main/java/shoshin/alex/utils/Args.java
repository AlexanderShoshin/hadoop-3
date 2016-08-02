package shoshin.alex.utils;

public class Args {
    public static void checkInput(String[] args, String... argsDescription) throws IllegalArgumentException {
        if (args.length != argsDescription.length) {
            StringBuilder message = new StringBuilder("Expected ")
                    .append(argsDescription.length)
                    .append(" input parameters: ");
            for (String descr: argsDescription) {
                message.append(descr).append(", ");
            }
            throw new IllegalArgumentException(message.toString());
        }
    }
}