package space.gorogoro.kubotanutil2;

import java.util.logging.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;

class BukkitUtil{
  static Logger log = Bukkit.getLogger();
  static Plugin plugin;

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

  // メッセージの送信とコマンドの実行は、メインスレッドで行う
  public static void sendMessage(Player p, String msg){
    runOnMain(() -> p.sendMessage(msg));
  }

  public static void dispatchCommand(String cmd){
    if ( !Bukkit.isPrimaryThread() && !plugin.isEnabled() ) {
      logWarn("Skipped the command because the plugin is disabled: " + cmd);
      return;
    }
    runOnMain(() -> Bukkit.dispatchCommand(Bukkit.getConsoleSender(), cmd));
  }

  private static void runOnMain(Runnable r){
    if ( Bukkit.isPrimaryThread() ) {
      r.run();
    } else if ( plugin.isEnabled() ) {
      Bukkit.getScheduler().runTask(plugin, r);
    }
  }

  // ディスクを読まないように、オンラインのプレイヤー → users テーブルの順で探す
  // online はメインスレッドで Bukkit.getPlayerExact(name) した結果
  public static OfflinePlayer getPlayer(Connection con, Player online, String name) throws SQLException {
    if ( online != null ) {
      return online;
    }
    try ( PreparedStatement prepStmt = con.prepareStatement(
        "SELECT uuid FROM users WHERE playername = ? COLLATE NOCASE ORDER BY id DESC LIMIT 1") ) {
      prepStmt.setString(1, name);
      try ( ResultSet rs = prepStmt.executeQuery() ) {
        if ( rs.next() ) {
          return Bukkit.getOfflinePlayer(UUID.fromString(rs.getString(1)));
        }
      }
    }
    return null;
  }
}