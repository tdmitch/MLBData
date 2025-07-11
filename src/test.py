import subprocess
import time

def force_reconnect_protonvpn_windows():
    openvpn_gui_path = r"C:\\Program Files\\Proton\\VPN\\ProtonVPN.Launcher.exe" # Adjust path if needed

    print("Disconnecting Proton VPN...")
    subprocess.Popen(f'"{openvpn_gui_path}" --command disconnect', shell=True)
    
    time.sleep(15) # Give time for the app to process disconnection

    print("Reconnecting Proton VPN...")
    subprocess.Popen(f'"{openvpn_gui_path}" --command reconnect', shell=True)


if __name__ == "__main__":
    force_reconnect_protonvpn_windows()