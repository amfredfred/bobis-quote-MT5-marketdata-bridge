$mt5Path = "c:\Program Files\FBS MetaTrader 5\terminal64.exe"
$WshShell = New-Object -ComObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut("$env:APPDATA\Microsoft\Windows\Start Menu\Programs\Startup\MT5.lnk")
$Shortcut.TargetPath = $mt5Path
$Shortcut.Save()