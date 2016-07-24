package shoshin.alex.utils;

public class Args {
    private String[] argsDescription;

    public Args(String... argsDescription) {
        this.argsDescription = argsDescription;
    }

    public void checkInput(String[] args) throws IllegalArgumentException {
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
