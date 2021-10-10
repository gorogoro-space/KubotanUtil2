package space.gorogoro.kubotanutil2;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;

public class KubotanUtil2 extends JavaPlugin implements Listener{
  private Connection con;

  @Override
  public void onEnable(){
    try{
      BukkitUtil.logInfo("The Plugin Has Been Enabled!");
      getServer().getPluginManager().registerEvents(this, this);

      // 設定ファイルが無ければ作成
      File configFile = new File(getDataFolder(), "config.yml");
      if(!configFile.exists()){
        this.saveDefaultConfig();
      }

      // JDBCドライバーの指定
      Class.forName("org.sqlite.JDBC");
      // データベースに接続する なければ作成される
      con = DriverManager.getConnection("jdbc:sqlite:" + getDataFolder() + File.separator + "status.db");
      con.setAutoCommit(false);      // auto commit無効

      // Statementオブジェクト作成
      Statement stmt = con.createStatement();
      stmt.setQueryTimeout(30);    // タイムアウト設定

      // テーブル作成
      stmt.executeUpdate("CREATE TABLE IF NOT EXISTS users ("
        + " id INTEGER PRIMARY KEY AUTOINCREMENT"
        + ",uuid STRING NOT NULL"
        + ",playername STRING NOT NULL"
        + ",reputation REAL NOT NULL DEFAULT 1.000"
        + ",reason STRING NOT NULL DEFAULT 'Empty for old data.'"
        + ",is_jailed INTEGER NOT NULL DEFAULT 0"
        + ",is_baned INTEGER NOT NULL DEFAULT 0"
        + ",last_executed_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(last_executed_at LIKE '____-__-__ __:__:__')"
        + ",created_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(created_at LIKE '____-__-__ __:__:__')"
        + ",updated_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(updated_at LIKE '____-__-__ __:__:__')"
        + ");"
      );
      stmt.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS uuid_uindex ON users (uuid);");
      stmt.executeUpdate("CREATE INDEX IF NOT EXISTS playername_index ON users (playername);");

      stmt.executeUpdate("CREATE TABLE IF NOT EXISTS last_executed_history ("
        + " id INTEGER PRIMARY KEY AUTOINCREMENT"
        + ",sender_uuid STRING NOT NULL"
        + ",sender_playername STRING NOT NULL"
        + ",target_uuid STRING NOT NULL"
        + ",target_playername STRING NOT NULL"
        + ",last_executed_type INTEGER NOT NULL"
        + ",reason STRING NOT NULL DEFAULT 'Empty for old data'"
        + ",created_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(created_at LIKE '____-__-__ __:__:__')"
        + ",updated_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(updated_at LIKE '____-__-__ __:__:__')"
        + ",unique(sender_uuid, target_uuid, last_executed_type)"
        + ");"
      );
      stmt.executeUpdate("CREATE INDEX IF NOT EXISTS last_uuid_type_index ON last_executed_history (sender_uuid, target_uuid, last_executed_type);");
      stmt.executeUpdate("CREATE INDEX IF NOT EXISTS sender_playername_index ON last_executed_history (sender_playername);");
      stmt.executeUpdate("CREATE INDEX IF NOT EXISTS target_playername_index ON last_executed_history (target_playername);");
      con.commit();
      stmt.close();

    } catch (Exception e){
      BukkitUtil.logStackMessage(e);
    }
  }

  @EventHandler
  public void onPlayerJoinEvent(PlayerJoinEvent event){
    try {
      // 既存ユーザーか確認
      String dbPlayerName; 
      String queryUpdateUser = "";
      Boolean existsUser = false;
      PreparedStatement prepStmt = con.prepareStatement("SELECT playername FROM users WHERE uuid=?");
      prepStmt.setString(1, event.getPlayer().getUniqueId().toString());
      ResultSet rs = prepStmt.executeQuery();
      if (rs.next()){
        existsUser = true;
        dbPlayerName = rs.getString(1);
        if(!dbPlayerName.equals(event.getPlayer().getName())) {
          queryUpdateUser = "UPDATE users SET playername=? WHERE uuid=?";
        }
      }
      rs.close();
      prepStmt.close();

      // 既存ユーザーで無ければユーザー登録
      if(!existsUser){
        prepStmt = con.prepareStatement(
          "INSERT INTO users("
          + " uuid"
          + ",playername"
          + ") VALUES ("
          + " ?"
          + ",?"
          + ");"
        );
        prepStmt.setString(1, event.getPlayer().getUniqueId().toString());
        prepStmt.setString(2, event.getPlayer().getName());
        prepStmt.addBatch();
        prepStmt.executeBatch();
        con.commit();
        prepStmt.close();
      }

      if(!queryUpdateUser.isEmpty()) {
        prepStmt = con.prepareStatement(queryUpdateUser);
        prepStmt.setString(1, event.getPlayer().getName());
        prepStmt.setString(2, event.getPlayer().getUniqueId().toString());
        prepStmt.addBatch();
        prepStmt.executeBatch();
        con.commit();
        prepStmt.close();
      }

      // 現在の情報を表示
      event.getPlayer().sendMessage("ようこそ " + event.getPlayer().getName() + " さん♪");
      MainProcess.status(con, event.getPlayer(), false);
    } catch (SQLException e) {
      BukkitUtil.logStackMessage(e);
    }
  }

  /**
   * コマンド実行時に呼び出されるメソッド
   * @see org.bukkit.plugin.java.JavaPlugin#onCommand(org.bukkit.command.CommandSender,
   *      org.bukkit.command.Command, java.lang.String, java.lang.String[])
   */
  public boolean onCommand( CommandSender sender, Command command, String label, String[] args) {
    try{
      if((sender instanceof Player)) {
        Player sp = (Player)sender;
        if(sp.hasPermission("space.gorogoro.kubotanutil2.default.*")){
          FileConfiguration conf = getConfig();
          if( command.getName().equals("bad") ) {
            return MainProcess.bad(conf, con, sp, args);
          }else if( command.getName().equals("good") ) {
            return MainProcess.good(conf, con, sp, args);
          }else if( command.getName().equals("status") ) {
            boolean ret;
            if(sp.isOp()){
              ret = MainProcess.statusOp(con, sp, args);
              if(sp.getName().equals("kubotan")){
                MainProcess.reasonOp(con, sp, args);
              }
            }else{
              ret = MainProcess.status(con, sp, true);
            }
            return ret;
          }else if( command.getName().equals("isjailed") ) {
            return MainProcess.isJailed(con, sp, args);
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kjail") && command.getName().equals("kjail") ) {
            return MainProcess.jail(this, con, conf, sp, args);
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kunjail") && command.getName().equals("kunjail") ) {
            return MainProcess.unjail(this, con, conf, sp, args);
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kban") && command.getName().equals("kban") ) {
            return MainProcess.ban(this, con, conf, sp, args);
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kunban") && command.getName().equals("kunban") ) {
            return MainProcess.unban(this, con, conf, sp, args);
          }else if( sp.isOp() && command.getName().equals("update") ) {
            return MainProcess.upDate(con, sp, args);
          }else if( sp.isOp() && command.getName().equals("upvalue") ) {
            return MainProcess.upValue(con, sp, args);
          }
        }
      }
    }catch(Exception e){
      BukkitUtil.logStackMessage(e);
    }
    return true;
  }

  @Override
  public void onDisable(){
    try{
      if (con != null) {
        con.close();      // DB切断
      }
      BukkitUtil.logInfo("The Plugin Has Been Disabled!");
    } catch (Exception e){
      BukkitUtil.logStackMessage(e);
    }
  }
}
