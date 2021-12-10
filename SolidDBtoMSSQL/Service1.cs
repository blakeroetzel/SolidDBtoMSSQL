using System;
using System.Collections.Generic;
using System.ServiceProcess;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Timers;
using System.Net.Mail;

namespace SolidDBtoMSSQL
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }
        Timer timer = new Timer();
        String startTime = " 07:00:00 PM";
        String versionNum = "v1.0";
        String serviceName = "SolidDB Backup Service";
        private static Dictionary<string, string> config = new Dictionary<string, string>();
        
        // Update this path to the path of your config file
        static string local = "C:/Dev/SolidDBtoMSSQL/.local/config.conf";
        
        public void readConfig()
        {
            using (StreamReader sr = new StreamReader(local + "config.conf"))
            {
                while (sr.Peek() >= 0)
                {
                    string line = sr.ReadLine();
                    var linesplit = line.Split('=');
                    config[linesplit[0]] = linesplit[1];
                }
            }
        }

        public void smtpMail(string messageText, string userName, string password)
        {
            SmtpClient client = new SmtpClient("email-smtp.us-east-1.amazonaws.com", 587);
            client.EnableSsl = true;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.Credentials = new System.Net.NetworkCredential(config["userName"], config["password"]);
            MailAddress from = new MailAddress("Tcectexas@roetzelfamily.com", "TCEC Error");
            MailAddress to = new MailAddress("broetzel@tcectexas.com");
            MailMessage message = new MailMessage(from, to);
            var host = Dns.GetHostEntry(Dns.GetHostName()).AddressList;
            messageText += $"\n{versionNum}";
            messageText = messageText + $"\n\nFrom {serviceName}. \n";
            var mName = Environment.MachineName;
            messageText += mName;
            foreach (var ip in host)
            {
                messageText = messageText + $"\n{ip.ToString()}";
            }
            message.Body = messageText;
            message.Subject = $"{serviceName} Error";
            try
            {
                client.Send(message);
            }
            catch (System.Net.Sockets.SocketException e)
            {
                WriteToFile($"{serviceName} Error: " + e.ToString());
            }
            catch (System.Net.Mail.SmtpException e)
            {
                WriteToFile($"{serviceName} Error: " + e.ToString());
            }
            client.Dispose();
        }

        public async void OnDebug()
        {
            readConfig();
            await new RunProgram().goAsync(config);
            GC.Collect();
        }

        protected override void OnStart(string[] args)
        {
            readConfig();
            WriteToFile(versionNum + "\n" + serviceName + " starting at " + DateTime.Now);
            timer.Elapsed += new ElapsedEventHandler(onElapsedTime);
            int milli = new int();
            if (DateTime.Now < DateTime.Parse(DateTime.Now.ToString("MM/dd/yyyy") + startTime))
            {
                milli = getToday();
            }
            else
            {
                milli = getNextRun();
            }
            timer.Interval = milli;
            WriteToFile("Next updating at " + DateTime.Now.AddMilliseconds(milli));
            timer.Enabled = true;
        }

        protected override void OnStop()
        {
            WriteToFile($"{serviceName} stopping at " + DateTime.Now);
        }

        private async void onElapsedTime(object source, ElapsedEventArgs e)
        {
            timer.Enabled = false;
            WriteToFile(serviceName + " running at " + DateTime.Now);
            await new RunProgram().goAsync(config);
            GC.Collect();
            int milli = getNextRun();
            timer.Interval = milli;
            WriteToFile("Next updating at " + DateTime.Now.AddMilliseconds(milli));
            timer.Enabled = true;
        }

        private int getNextRun()
        {
            DateTime dt1 = DateTime.Now;
            DateTime dt2 = DateTime.Parse(DateTime.Now.AddDays(1).ToString("MM/dd/yyyy") + startTime);
            TimeSpan span = dt2 - dt1;
            int milli = (int)span.TotalMilliseconds;
            return milli;
        }

        private int getToday()
        {
            DateTime dt1 = DateTime.Now;
            DateTime dt2 = DateTime.Parse(DateTime.Now.ToString("MM/dd/yyyy") + startTime);
            TimeSpan span = dt2 - dt1;
            int milli = (int)span.TotalMilliseconds;
            return milli;
        }

        public async void WriteToFile(string Message)
        {
            string path = "C:\\Dev\\";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            // Update This Name!
            string filepath = "C:\\Dev\\SolidDBtoMSSQL.txt";
            if (!File.Exists(filepath))
            {
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + ": " + Message);
                }
            }
            else
            {
                try
                {
                    using (StreamWriter sw = File.AppendText(filepath))
                    {
                        sw.WriteLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss") + ": " + Message);
                    }
                } catch (System.IO.IOException e)
                {
                    await Task.Delay(10);
                    WriteToFile(Message);
                }
            }
        }
    }
}
