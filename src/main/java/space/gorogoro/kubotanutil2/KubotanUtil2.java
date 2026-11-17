package space.gorogoro.kubotanutil2;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;

public class KubotanUtil2 extends JavaPlugin implements Listener{
  private static final long DB_SHUTDOWN_TIMEOUT_SECONDS = 5;
  private Connection con;
  private ExecutorService dbExecutor;

  @Override
  public void onEnable(){
    try{
      BukkitUtil.plugin = this;
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
        + ",is_jailed INTEGER NOT NULL DEFAULT 0"
        + ",is_baned INTEGER NOT NULL DEFAULT 0"
        + ",created_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(created_at LIKE '____-__-__ __:__:__')"
        + ",updated_at DATETIME NOT NULL DEFAULT (datetime('now','localtime')) CHECK(updated_at LIKE '____-__-__ __:__:__')"
        + ");"
      );
      stmt.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS uuid_uindex ON users (uuid);");
      stmt.executeUpdate("CREATE INDEX IF NOT EXISTS playername_index ON users (playername);");
      stmt.executeUpdate("CREATE INDEX IF NOT EXISTS playername_nocase_index ON users (playername COLLATE NOCASE);");

      con.commit();
      stmt.close();

      dbExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "KubotanUtil2-DB"));
      runDb(this::removeReputationData);

    } catch (Exception e){
      BukkitUtil.logStackMessage(e);
    }
  }

  // DB の読み書きは、メインスレッドを止めないようにこのスレッドで順番に行う
  private void runDb(DbTask task){
    if(dbExecutor == null || dbExecutor.isShutdown()){
      return;
    }
    dbExecutor.execute(() -> {
      try{
        task.run();
      }catch(Exception e){
        BukkitUtil.logStackMessage(e);
      }
    });
  }

  private interface DbTask {
    void run() throws Exception;
  }

  // 2.0 だけの処理: 廃止した評価制度のデータを status.db から削除する(2.1 でこの処理ごと削除する)
  // 削除するものが無ければ何もしない
  private void removeReputationData() throws SQLException {
    List<String> columns = new ArrayList<>();
    try (Statement stmt = con.createStatement();
         ResultSet rs = stmt.executeQuery("PRAGMA table_info(users)")) {
      while (rs.next()) {
        columns.add(rs.getString("name"));
      }
    }
    boolean hasHistory;
    try (Statement stmt = con.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='last_executed_history'")) {
      hasHistory = rs.next();
    }

    List<String> removed = new ArrayList<>();
    try (Statement stmt = con.createStatement()) {
      for (String column : new String[]{"reputation", "reason", "last_executed_at"}) {
        if (columns.contains(column)) {
          stmt.executeUpdate("ALTER TABLE users DROP COLUMN " + column);
          removed.add("users." + column);
        }
      }
      if (hasHistory) {
        // 索引もテーブルと一緒に削除される
        stmt.executeUpdate("DROP TABLE last_executed_history");
        removed.add("last_executed_history");
      }
      con.commit();
    } catch (SQLException e) {
      con.rollback();
      throw e;
    }
    if (removed.isEmpty()) {
      return;
    }

    // 削除したデータがファイルに残らないように詰める(VACUUM はトランザクションの外で行う)
    con.setAutoCommit(true);
    try (Statement stmt = con.createStatement()) {
      stmt.executeUpdate("VACUUM");
    } finally {
      con.setAutoCommit(false);
    }
    BukkitUtil.logInfo("Removed the reputation data from status.db: " + String.join(", ", removed));
  }

  @EventHandler
  public void onPlayerJoinEvent(PlayerJoinEvent event){
    Player player = event.getPlayer();
    String uuid = player.getUniqueId().toString();
    String name = player.getName();
    runDb(() -> join(uuid, name));
  }

  private void join(String uuid, String name){
    try {
      // 既存ユーザーか確認
      String dbPlayerName;
      String queryUpdateUser = "";
      Boolean existsUser = false;
      PreparedStatement prepStmt = con.prepareStatement("SELECT playername FROM users WHERE uuid=?");
      prepStmt.setString(1, uuid);
      ResultSet rs = prepStmt.executeQuery();
      if (rs.next()){
        existsUser = true;
        dbPlayerName = rs.getString(1);
        if(!dbPlayerName.equals(name)) {
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
        prepStmt.setString(1, uuid);
        prepStmt.setString(2, name);
        prepStmt.addBatch();
        prepStmt.executeBatch();
        con.commit();
        prepStmt.close();
      }

      if(!queryUpdateUser.isEmpty()) {
        prepStmt = con.prepareStatement(queryUpdateUser);
        prepStmt.setString(1, name);
        prepStmt.setString(2, uuid);
        prepStmt.addBatch();
        prepStmt.executeBatch();
        con.commit();
        prepStmt.close();
      }
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
          // 引数の数の確認とオンラインのプレイヤーの検索はメインスレッドで行い、DB の処理は runDb に渡す
          FileConfiguration conf = getConfig();
          Player online = args.length > 0 ? Bukkit.getPlayerExact(args[0]) : null;
          if( command.getName().equals("isjailed") ) {
            if(args.length != 1){
              return false;
            }
            runDb(() -> MainProcess.isJailed(con, sp, args));
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kjail") && command.getName().equals("kjail") ) {
            if(args.length != 1){
              return false;
            }
            runDb(() -> MainProcess.jail(this, con, conf, sp, online, args));
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kunjail") && command.getName().equals("kunjail") ) {
            if(args.length != 1){
              return false;
            }
            runDb(() -> MainProcess.unjail(this, con, conf, sp, online, args));
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kban") && command.getName().equals("kban") ) {
            if(args.length != 2){
              return false;
            }
            runDb(() -> MainProcess.ban(this, con, conf, sp, args));
          }else if( sp.hasPermission("space.gorogoro.kubotanutil2.kunban") && command.getName().equals("kunban") ) {
            if(args.length != 1){
              return false;
            }
            runDb(() -> MainProcess.unban(this, con, conf, sp, args));
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
      // 残っている DB の処理を最大 5 秒待つ
      if (dbExecutor != null) {
        dbExecutor.shutdown();
        if (!dbExecutor.awaitTermination(DB_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          BukkitUtil.logWarn("Timed out waiting for the database tasks. Closing the database.");
          dbExecutor.shutdownNow();
        }
      }
      if (con != null) {
        con.close();      // DB切断
      }
      BukkitUtil.logInfo("The Plugin Has Been Disabled!");
    } catch (Exception e){
      BukkitUtil.logStackMessage(e);
    }
  }
}
