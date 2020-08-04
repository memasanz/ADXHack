using System;
using System.Collections.Generic;
using System.Text;

namespace AnomonlyDetectionConsoleApp
{
    public class CHRMessage
    {
        public DateTime processed { get; set; }

        public string transactionType { get; set; }

        public string direction { get; set; }

        public string partner { get; set; }

        public string serverClusterMainNode { get; set; }

        public int errorResolutionType { get; set; }

        public string purpose { get; set; }

        public string loadNum { get; set; }

        public string shipmentNum { get; set; }

        public string proNum { get; set; }
    }

}
