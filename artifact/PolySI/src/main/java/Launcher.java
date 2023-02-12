import com.jdotsoft.jarloader.JarClassLoader;

import lombok.SneakyThrows;

public class Launcher {
    @SneakyThrows
    public static void main(String[] args) {
        var loader = new JarClassLoader();
        loader.invokeMain("Main", args);
    }
}
