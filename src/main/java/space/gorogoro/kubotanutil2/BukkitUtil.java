package space.gorogoro.kubotanutil2;

import java.util.logging.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;

class BukkitUtil{
  static Logger log = Bukkit.getLogger();

  public static void logWarn(String str){
    log.warning(str);
  }

  public static void logInfo(String str){
    log.info(str);
  }

  public static void logStackMessage(Exception e){
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    pw.flush();
    log.warning(sw.toString());
  }

  public static OfflinePlayer getPlayer(String name) {
    for ( OfflinePlayer player : Bukkit.getOfflinePlayers() ) {
        if ( player.getName().equals(name) ) {
            return player;
        }
    }
    return null;
  }
}