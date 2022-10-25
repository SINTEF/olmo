import time
import board
from adafruit_ina219 import ADCResolution, BusVoltageRange, INA219
import relay_lib_seeed as rl

i2c_bus = board.I2C()


class Channel:
    "This is a channel class from the power control unit."

    def __init__(self, channel):
        try:
            self.name = channel['name']
            self.RelayNumber = channel['relay']
            self.SensorI2CAddress = channel['i2c_address']
            self.init_sensor()
            print("Channel with relay {0} and sensor address {1} set up with name \"{2}\".".format(self.RelayNumber,
                                                                                                   hex(self.SensorI2CAddress),
                                                                                                   self.name))
            state = self.turn_off()
        except:
            print("""
            invalid channel, channel mus be in the format
            1:{'name': 'camera', 'relay': 1, 'i2c_address': 0x40},
            """)

    def init_sensor(self):
        self.ina219 = INA219(i2c_bus, self.SensorI2CAddress)
        # optional : change configuration to use 32 samples averaging for both bus voltage and shunt voltage
        self.ina219.bus_adc_resolution = ADCResolution.ADCRES_12BIT_32S
        self.ina219.shunt_adc_resolution = ADCResolution.ADCRES_12BIT_32S
        # optional : change voltage range to 16V
        self.ina219.bus_voltage_range = BusVoltageRange.RANGE_16V

    def read_voltage(self):
        self.Voltage = self.ina219.bus_voltage  # voltage on V- (load side)
        return self.Voltage

    def read_current(self):
        self.Current = self.ina219.current  # current in mA
        return self.Current

    def read_power_calc(self):
        self.Power_calc = self.read_voltage() * self.read_current()  # power in mW
        return self.Power_calc

    def read_power(self):
        self.Power = self.ina219.power  # power in mW
        return self.Power

    def print_voltage(self):
        print("Relay {0} voltage is {1} V".format(self.RelayNumber, self.read_voltage()))

    def read_supply_voltage(self):
        self.Voltage = self.ina219.bus_voltage
        self.Voltage_shunt = self.ina219.shunt_voltage
        self.Voltage_supply = self.Voltage + self.Voltage_shunt
        return self.Voltage_supply

    def read_shunt_voltage(self):
        self.Voltage_shunt = self.ina219.shunt_voltage
        return self.Voltage_shunt

    def turn_on(self):
        rl.relay_on(self.RelayNumber)
        self.state = "ON"
        return self.state

    def turn_off(self):
        rl.relay_off(self.RelayNumber)
        self.state = "OFF"
        return self.state


def setup_board(channels):
    board = dict()
    for k, v in channels.items():
        # board[k] = Channel(v['relay'],v['i2c_address'])
        board[k] = Channel(v)
    return board


def test_board(board):
    print("Starting to toggle channels")
    for channel in board:
        board[channel].turn_on()
        time.sleep(1)
        board[channel].print_voltage()
        time.sleep(1)

        board[channel].turn_off()
        time.sleep(1)
        board[channel].print_voltage()
        time.sleep(1)

        print("Board test complete")


if __name__ == "__main__":
    # channel confiuration of the board, assigning relays to sensors
    channels = {
        1: {'name': 'camera', 'relay': 1, 'i2c_address': 0x40},
        2: {'name': 'pi', 'relay': 2, 'i2c_address': 0x41},
        3: {'name': 'ctd', 'relay': 3, 'i2c_address': 0x44},
        4: {'name': 'adctp', 'relay': 4, 'i2c_address': 0x45}
    }

    # test functions
    demo_board = setup_board(channels)
    test_board(demo_board)
