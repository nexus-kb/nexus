\set ON_ERROR_STOP on

-- Repairs the malformed Date headers currently present in the LKML import.
-- corrected_sent_at is the public-inbox commit time at which each message was
-- delivered. This is preferable to the malformed sender Date and agrees with
-- the surrounding messages in multi-message threads.
--
-- Run with:
--   psql -X --file deploy/fix-malformed-message-timestamps.sql

BEGIN;

CREATE TEMP TABLE message_timestamp_repairs (
    message_id text PRIMARY KEY,
    old_sent_at timestamptz,
    corrected_sent_at timestamptz NOT NULL
) ON COMMIT DROP;

INSERT INTO message_timestamp_repairs (
    message_id,
    old_sent_at,
    corrected_sent_at
)
VALUES
    ('FFFFFFFF9BFD9A66.7E0E71C@hawaii.rr.com', timestamptz '1916-10-31 02:35:19+00', timestamptz '2000-12-06T04:41:44-05:00'),
    ('19031231222908.21882@mailhost.mipsys.com', timestamptz '1903-12-31 22:29:08+00', timestamptz '2001-04-19T06:08:57-04:00'),
    ('19040107093209.B1421@hbe.ca', timestamptz '1904-01-07 08:32:10+00', timestamptz '2001-12-22T15:22:13-05:00'),
    ('9DB9E775-1E2E-11B2-BD7E-003065455956@e2-media.co.nz', timestamptz '1970-01-01 11:02:25+00', timestamptz '2002-01-31T15:02:46-05:00'),
    ('19040101001954.51000b1d.rusty@rustcorp.com.au', timestamptz '1903-12-31 14:19:53+00', timestamptz '2002-02-26T20:40:11-05:00'),
    ('197603031558.G23FwZY05020@www.hockin.org', timestamptz '1976-03-03 15:58:35+00', timestamptz '2002-03-10T18:56:10-05:00'),
    ('19031231230432.20732@smtp.wanadoo.fr', timestamptz '1903-12-31 23:04:32+00', timestamptz '2002-05-08T07:00:13-04:00'),
    ('1054563475.0@localhost.localdomain', timestamptz '1970-01-13 06:22:13+00', timestamptz '2003-06-02T10:04:09-04:00'),
    ('19040101015154.GA346@libero.it', timestamptz '1904-01-01 01:51:54+00', timestamptz '2003-06-05T16:55:20-04:00'),
    ('19040101002501.GA247@libero.it', timestamptz '1904-01-01 00:25:01+00', timestamptz '2003-06-06T16:59:38-04:00'),
    ('E19rPUc-0003Nv-2M@out-mta1.plasa.com', timestamptz '1980-01-19 15:18:21+00', timestamptz '2003-08-25T18:07:45-04:00'),
    ('19031231230857.GA1050@suse.de', timestamptz '1903-12-31 23:08:57+00', timestamptz '2003-09-21T05:23:50-04:00'),
    ('807dy30$yh9e8$989xi08---2@y6d4.v.m2.pau2', timestamptz '1970-01-01 01:00:00+00', timestamptz '2003-12-08T14:10:15-05:00'),
    ('8D2C6B0D-21C8-11B2-9DDD-000A958B60EE@runbox.com', timestamptz '1970-01-06 01:01:53+00', timestamptz '2004-01-24T03:51:20-05:00'),
    ('20050202161845.976934000@blunzn.suse.de', timestamptz '1931-08-26 01:52:25+00', timestamptz '2005-02-02T11:25:31-05:00'),
    ('20050202161846.093204000@blunzn.suse.de', timestamptz '1931-08-26 01:51:25+00', timestamptz '2005-02-02T11:25:00-05:00'),
    ('20050202161845.866892000@blunzn.suse.de', timestamptz '1931-08-26 01:53:25+00', timestamptz '2005-02-02T11:29:24-05:00'),
    ('19031231170827.GA3610@hugang.soulinfo.com', timestamptz '1903-12-31 17:08:47+00', timestamptz '2005-02-13T00:54:05-05:00'),
    ('Pine.LNX.4.61.6912311752220.4361@chaos.analogic.com', timestamptz '1969-12-31 22:54:41+00', timestamptz '2005-05-13T08:17:40-04:00'),
    ('19700102031329.GA2372@ucw.cz', timestamptz '1970-01-02 03:13:29+00', timestamptz '2005-12-13T16:12:55-05:00'),
    ('19700102032843.GA2445@ucw.cz', timestamptz '1970-01-02 03:28:43+00', timestamptz '2005-12-13T16:12:56-05:00'),
    ('19700101132635.GB3561@ucw.cz', timestamptz '1970-01-01 13:26:35+00', timestamptz '2006-07-16T10:33:58-04:00'),
    ('19700101001522.GA3999@ucw.cz', timestamptz '1970-01-01 00:15:22+00', timestamptz '2006-08-06T18:00:53-04:00'),
    ('19700101001658.GA4066@ucw.cz', timestamptz '1970-01-01 00:16:58+00', timestamptz '2006-09-01T10:23:33-04:00'),
    ('6D63B142.2080404@linux.vnet.ibm.com', timestamptz '2028-02-27 07:39:46+00', timestamptz '2007-06-25T08:44:26-04:00'),
    ('20071019220007.791059000@linux.vnet.ibm.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2007-10-19T18:02:15-04:00'),
    ('48611ec1.2234440a.679b.1a7d@mx.google.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2008-06-24T12:21:59-04:00'),
    ('48459207.0637560a.672f.1b0d@mx.google.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2008-06-03T14:48:51-04:00'),
    ('48459207.0407560a.301b.231b@mx.google.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2008-06-03T14:49:30-04:00'),
    ('48611ec1.2234440a.679b.1a7c@mx.google.com', timestamptz '1970-01-01 00:00:03+00', timestamptz '2008-06-24T12:20:31-04:00'),
    ('48611ec2.1636440a.75ee.2503@mx.google.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2008-06-24T12:20:45-04:00'),
    ('48611ec5.2435440a.32c4.1a96@mx.google.com', timestamptz '1970-01-01 00:00:05+00', timestamptz '2008-06-24T12:21:27-04:00'),
    ('48611ec4.1836440a.79e4.22e4@mx.google.com', timestamptz '1970-01-01 00:00:04+00', timestamptz '2008-06-24T12:20:57-04:00'),
    ('488866b0.2233440a.6886.ffff9b13@mx.google.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2008-07-24T07:26:03-04:00'),
    ('488866bf.1636440a.2c8e.ffffa160@mx.google.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2008-07-24T07:26:27-04:00'),
    ('20080724114350.624430013@gmail.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2008-07-24T07:44:24-04:00'),
    ('20080724114350.444031862@gmail.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2008-07-24T07:44:39-04:00'),
    ('1912217169.25608.228.camel@ymzhang', timestamptz '2030-08-06 03:26:09+00', timestamptz '2008-08-05T23:26:37-04:00'),
    ('1912924600.25608.298.camel@ymzhang', timestamptz '2030-08-14 07:56:40+00', timestamptz '2008-08-14T03:58:58-04:00'),
    ('20080814184651.294623640@gmail.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2008-08-14T14:47:09-04:00'),
    ('20080814184651.471268866@gmail.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2008-08-14T14:47:27-04:00'),
    ('20080814184651.681143113@gmail.com', timestamptz '1970-01-01 00:00:03+00', timestamptz '2008-08-14T14:47:39-04:00'),
    ('20080814184652.179229281@gmail.com', timestamptz '1970-01-01 00:00:06+00', timestamptz '2008-08-14T14:48:18-04:00'),
    ('20080814184651.853370625@gmail.com', timestamptz '1970-01-01 00:00:04+00', timestamptz '2008-08-14T14:48:51-04:00'),
    ('20080814184652.022151205@gmail.com', timestamptz '1970-01-01 00:00:05+00', timestamptz '2008-08-14T14:48:34-04:00'),
    ('20080814184652.317090901@gmail.com', timestamptz '1970-01-01 00:00:07+00', timestamptz '2008-08-14T14:49:13-04:00'),
    ('20080814184652.475361864@gmail.com', timestamptz '1970-01-01 00:00:08+00', timestamptz '2008-08-14T14:49:37-04:00'),
    ('1912841442.25608.284.camel@ymzhang', timestamptz '2030-08-13 08:50:42+00', timestamptz '2008-08-13T04:52:49-04:00'),
    ('4916c290.0437560a.270e.0940@mx.google.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2008-11-09T05:59:56-05:00'),
    ('4916c290.0c07560a.4a14.ffffdde9@mx.google.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2008-11-09T06:00:23-05:00'),
    ('19700101001343.GA1440@ucw.cz', timestamptz '1970-01-01 00:13:43+00', timestamptz '2008-12-14T13:08:19-05:00'),
    ('19700101064603.GA1406@ucw.cz', timestamptz '1970-01-01 06:46:03+00', timestamptz '2008-12-26T13:24:35-05:00'),
    ('19700101065033.GB1406@ucw.cz', timestamptz '1970-01-01 06:50:33+00', timestamptz '2008-12-26T13:24:21-05:00'),
    ('4975dc14.0637560a.7f8e.729c@mx.google.com', timestamptz '1970-01-01 00:00:05+00', timestamptz '2009-01-20T09:15:26-05:00'),
    ('4975dc12.0c07560a.7d35.ffffcc78@mx.google.com', timestamptz '1970-01-01 00:00:03+00', timestamptz '2009-01-20T09:15:51-05:00'),
    ('4975dc11.1358560a.19ec.7fe3@mx.google.com', timestamptz '1970-01-01 00:00:01+00', timestamptz '2009-01-20T09:14:56-05:00'),
    ('4975dc12.0637560a.214c.34ce@mx.google.com', timestamptz '1970-01-01 00:00:02+00', timestamptz '2009-01-20T09:16:20-05:00'),
    ('4975dc12.1438560a.5c3c.403b@mx.google.com', timestamptz '1970-01-01 00:00:04+00', timestamptz '2009-01-20T09:16:46-05:00'),
    ('1902387910.2078.435.camel@ymzhang.sh.intel.com', timestamptz '2030-04-14 09:05:10+00', timestamptz '2010-04-14T05:05:35-04:00'),
    ('1902445479.2078.458.camel@ymzhang.sh.intel.com', timestamptz '2030-04-15 01:04:39+00', timestamptz '2010-04-14T21:04:57-04:00'),
    ('1902473858.2078.481.camel@ymzhang.sh.intel.com', timestamptz '2030-04-15 08:57:38+00', timestamptz '2010-04-15T04:57:54-04:00'),
    ('315925806-1352-1-git-send-email-josright123@gmail.com', timestamptz '1980-01-05 13:10:06+00', timestamptz '2013-03-27T02:42:39-04:00'),
    ('2236503749-2998-1-git-send-email-shuah.kh@samsung.com', timestamptz '2040-11-14 11:02:29+00', timestamptz '2013-05-31T23:26:46-04:00'),
    ('2236505237-3336-1-git-send-email-shuah.kh@samsung.com', timestamptz '2040-11-14 11:27:17+00', timestamptz '2013-05-31T23:58:39-04:00'),
    ('19700101000313.GA1434@xo-6d-61-c0.localdomain', timestamptz '1970-01-01 00:03:13+00', timestamptz '2013-10-24T04:27:18-04:00'),
    ('190456-3054-1-git-send-email-yuwang.yu@huawei.com', timestamptz '1970-01-03 04:54:16+00', timestamptz '2015-05-13T21:42:41-04:00'),
    ('190487-3155-1-git-send-email-yuwang.yu@huawei.com', timestamptz '1970-01-03 04:54:47+00', timestamptz '2015-05-13T21:43:12-04:00'),
    ('3578876466-3733-1-git-send-email-nj.shetty@samsung.com', timestamptz '2083-05-30 04:21:06+00', timestamptz '2018-02-08T09:37:02-05:00'),
    ('20850618155720.24857-1-Amy.Shih@advantech.com.tw', timestamptz '2085-06-18 15:57:19+00', timestamptz '2019-09-02T10:06:22+00:00'),
    ('20421230130811.2542-1-xingtong_wu@163.com', timestamptz '2042-12-30 13:08:10+00', timestamptz '2023-05-26T02:58:45+00:00'),
    ('20430731173417.2692-1-xingtong_wu@163.com', timestamptz '2043-07-31 17:34:17+00', timestamptz '2023-08-07T03:41:23+00:00'),
    ('20430731173026.2631-1-xingtong_wu@163.com', timestamptz '2043-07-31 17:30:25+00', timestamptz '2023-08-07T03:52:26+00:00'),
    ('20430731173026.2631-2-xingtong_wu@163.com', timestamptz '2043-07-31 17:30:26+00', timestamptz '2023-08-07T03:52:41+00:00'),
    ('20430802173515.2363-1-xingtong_wu@163.com', timestamptz '2043-08-02 17:35:14+00', timestamptz '2023-08-09T03:42:13+00:00'),
    ('20430802173515.2363-2-xingtong_wu@163.com', timestamptz '2043-08-02 17:35:15+00', timestamptz '2023-08-09T03:42:15+00:00'),
    ('20430802173844.2483-1-xingtong_wu@163.com', timestamptz '2043-08-02 17:38:44+00', timestamptz '2023-08-09T03:45:41+00:00'),
    ('bef5a084-9a6c-daaf-dad9-89cb5fc32bf9@163.com', timestamptz '2043-08-27 17:17:41+00', timestamptz '2023-10-11T08:47:57+00:00'),
    ('20770915-nolibc-run-user-v1-0-3caec61726dc@weissschuh.net', timestamptz '2077-09-15 00:13:51+00', timestamptz '2023-11-08T17:40:14+00:00'),
    ('20770915-nolibc-run-user-v1-2-3caec61726dc@weissschuh.net', timestamptz '2077-09-15 00:13:53+00', timestamptz '2023-11-08T17:40:09+00:00'),
    ('20770915-nolibc-run-user-v1-1-3caec61726dc@weissschuh.net', timestamptz '2077-09-15 00:13:52+00', timestamptz '2023-11-08T17:40:11+00:00'),
    ('20260922-for-upstream-lynx-25gbaser-v1-v2-1-1e6cf79c7b52@free.fr', timestamptz '2026-09-22 19:40:33+00', timestamptz '2026-09-01T18:18:22+00:00'),
    ('20000902155021.51C9E4917@pornstar.not.very.secure.org', NULL, timestamptz '2000-09-02T12:44:56-04:00'),
    ('312.887309.876868@', NULL, timestamptz '2000-09-03T05:17:36-04:00'),
    ('200011020106.UAA06749@blaze.usu.net', NULL, timestamptz '2000-11-01T19:57:32-05:00'),
    ('605.543077.239359@hotmail.com', NULL, timestamptz '2000-11-06T19:37:36-05:00'),
    ('200012100910.EAA28516@smarty.smart.net', NULL, timestamptz '2000-12-10T04:41:21-05:00'),
    ('200101040552.AAA24643@smarty.smart.net', NULL, timestamptz '2001-01-04T00:52:44-05:00'),
    ('200101240418.CAA24985@cactus.casm.ufsm.br', NULL, timestamptz '2001-01-23T23:23:58-05:00'),
    ('20010122020147Z130431-18594+148@vger.kernel.org', NULL, timestamptz '2001-01-21T21:02:01-05:00'),
    ('20010127134127Z131224-460+582@vger.kernel.org', NULL, timestamptz '2001-01-27T08:41:41-05:00'),
    ('20010129015821Z136038-460+994@vger.kernel.org', NULL, timestamptz '2001-01-28T20:58:39-05:00'),
    ('101.630009.718089@mail.wpgsun.com', NULL, timestamptz '2001-01-31T23:31:56-05:00'),
    ('564.402242.750161@unknown', NULL, timestamptz '2001-02-07T20:34:19-05:00'),
    ('93.98083.115261@unknown', NULL, timestamptz '2001-02-11T16:59:05-05:00'),
    ('20010219004729Z129994-513+7797@vger.kernel.org', NULL, timestamptz '2001-02-18T19:47:50-05:00'),
    ('20010305205914Z130662-407+1559@vger.kernel.org', NULL, timestamptz '2001-03-05T15:59:33-05:00'),
    ('200103060912.KAA09397@office.mandrakesoft.com', NULL, timestamptz '2001-03-06T04:13:14-05:00'),
    ('200103282334.JAA14009@vus068.trl.telstra.com.au', NULL, timestamptz '2001-03-28T18:37:34-05:00'),
    ('20010402221831Z131407-407+5602@vger.kernel.org', NULL, timestamptz '2001-04-02T18:18:50-04:00'),
    ('876.707558.146089@safestory.com', NULL, timestamptz '2001-04-24T10:18:18-04:00'),
    ('855.435960.224100@', NULL, timestamptz '2001-04-28T10:17:39-04:00'),
    ('200104192012.f3JKCZa02848@linux.ncport.ru', NULL, timestamptz '2001-04-19T16:17:29-04:00'),
    ('20010604221030.27042.qmail@pc7.prs.nunet.net', NULL, timestamptz '2001-06-04T18:10:57-04:00'),
    ('200106230054.f5N0sDJ02195@kahuna.cag.cpqcorp.net', NULL, timestamptz '2001-06-22T21:09:48-04:00'),
    ('790.623323.849760@asdfgh@yahoo.co.kr', NULL, timestamptz '2001-06-27T01:05:17-04:00'),
    ('20010706212351Z266854-17721+9046@vger.kernel.org', NULL, timestamptz '2001-07-06T17:24:22-04:00'),
    ('MDA2OT.TWlrZSBDcmF3Zm9yZA@melfina.tuxnami.org', NULL, timestamptz '2001-07-10T19:52:18-04:00'),
    ('20010724083909.12454.qmail@corpemail.asiaonline.net.my', NULL, timestamptz '2001-07-24T04:25:21-04:00'),
    ('200107250824.f6P8Oav126718@westrelay03.boulder.ibm.com', NULL, timestamptz '2001-07-25T04:24:56-04:00'),
    ('21.343299.814378@cardtown.com', NULL, timestamptz '2001-08-20T06:14:42-04:00'),
    ('BAKY8PDQGodzKCUgm7l0002c39a@bak.NMB.PNONLINE.COM', NULL, timestamptz '2001-09-27T07:32:12-04:00'),
    ('MDAyNz.RnJhbmsgRmllbm@mail.veka.com', NULL, timestamptz '2001-09-27T08:08:46-04:00'),
    ('20011215184608.45EC81D7B4953@sm4.163.com', NULL, timestamptz '2001-12-16T12:01:07-05:00'),
    ('200201260343.g0Q3hup19024@www4.mailbr.com.br', NULL, timestamptz '2002-01-25T22:44:37-05:00'),
    ('841.35276.723506@yahoo.com', NULL, timestamptz '2002-02-12T08:30:41-05:00'),
    ('20020215012612Z282902-13996+23554@vger.kernel.org', NULL, timestamptz '2002-02-14T20:26:24-05:00'),
    ('200202192012.g1JKCOd15393@kahuna.cag.cpqcorp.net', NULL, timestamptz '2002-02-19T15:35:17-05:00'),
    ('673.587866.856462@rn.com', NULL, timestamptz '2002-02-27T20:43:55-05:00'),
    ('200203010250.g212ofF25736@kahuna.cag.cpqcorp.net', NULL, timestamptz '2002-02-28T22:39:24-05:00'),
    ('20020425033549Z312889-22651+16570@vger.kernel.org', NULL, timestamptz '2002-04-24T23:35:51-04:00'),
    ('20020512140039Z313384-22651+30365@vger.kernel.org', NULL, timestamptz '2002-05-12T10:00:41-04:00'),
    ('20020526041148Z315690-22651+57424@vger.kernel.org', NULL, timestamptz '2002-05-26T00:11:49-04:00'),
    ('20020531012848Z314227-22651+70622@vger.kernel.org', NULL, timestamptz '2002-05-30T21:28:50-04:00'),
    ('20020523014954Z315793-22651+49629@vger.kernel.org', NULL, timestamptz '2002-05-22T21:49:55-04:00'),
    ('005e25e61e3e$4551a1c5$7bd26dc8@hkpsil', NULL, timestamptz '2002-06-17T16:47:53-04:00'),
    ('20020625003703Z315424-22020+10192@vger.kernel.org', NULL, timestamptz '2002-06-24T20:37:04-04:00'),
    ('001c61c53d8b$1666a4b5$0bb15cd4@exuhis', NULL, timestamptz '2002-06-26T22:52:19-04:00'),
    ('20020623101537Z316982-22020+9048@vger.kernel.org', NULL, timestamptz '2002-06-23T06:15:39-04:00'),
    ('20020607100510Z317265-22020+534@vger.kernel.org', NULL, timestamptz '2002-06-07T06:05:11-04:00'),
    ('20020612014409Z317303-22020+2655@vger.kernel.org', NULL, timestamptz '2002-06-11T21:44:10-04:00'),
    ('012e66e88b7c$7742a8a3$2ce80ab7@ahudof', NULL, timestamptz '2002-06-12T16:07:38-04:00'),
    ('038c82e53e5d$6436b8e1$2eb88bc0@xgeqwy', NULL, timestamptz '2002-06-13T05:18:59-04:00'),
    ('20020703044048Z316900-685+2443@vger.kernel.org', NULL, timestamptz '2002-07-03T00:40:50-04:00'),
    ('20020704045422Z317334-685+2768@vger.kernel.org', NULL, timestamptz '2002-07-04T00:54:24-04:00'),
    ('20020711023033Z317723-686+1039@vger.kernel.org', NULL, timestamptz '2002-07-10T22:30:36-04:00'),
    ('20020724003728Z315455-685+16862@vger.kernel.org', NULL, timestamptz '2002-07-23T20:37:29-04:00'),
    ('17Z6r9-1mkrJYC@fwd09.sul.t-online.com', NULL, timestamptz '2002-07-29T05:27:21-04:00'),
    ('20020723222004Z318215-685+16770@vger.kernel.org', NULL, timestamptz '2002-07-23T18:20:04-04:00'),
    ('20020808200000Z317950-686+4683@vger.kernel.org', NULL, timestamptz '2002-08-08T16:00:03-04:00'),
    ('20020828064744.A3AB4BFE8@rekin.go2.pl', NULL, timestamptz '2002-08-28T02:43:29-04:00'),
    ('20020828064718.225E4C22A@rekin.go2.pl', NULL, timestamptz '2002-08-28T02:42:59-04:00'),
    ('022e55d02c4c$7478d4c5$3dc23cb8@xdcham', NULL, timestamptz '2002-08-29T12:49:11-04:00'),
    ('007a68d60d6a$8441c0c7$4ea45eb7@niqvic', NULL, timestamptz '2002-09-17T22:56:33-04:00'),
    ('20020918221109Z269205-685+50304@vger.kernel.org', NULL, timestamptz '2002-09-18T18:11:10-04:00'),
    ('20020923041059Z264743-685+52399@vger.kernel.org', NULL, timestamptz '2002-09-23T00:10:59-04:00'),
    ('20020926030101Z262152-8740+1301@vger.kernel.org', NULL, timestamptz '2002-09-25T23:01:02-04:00'),
    ('20021004092853Z261542-8740+6087@vger.kernel.org', NULL, timestamptz '2002-10-04T05:28:55-04:00'),
    ('027e65d38c1c$6482d6c0$6ab01cb4@uthvqt', NULL, timestamptz '2002-10-12T16:37:03-04:00'),
    ('20021030035858Z263760-32597+11963@vger.kernel.org', NULL, timestamptz '2002-10-29T22:58:59-05:00'),
    ('20021031044813Z265093-32597+12699@vger.kernel.org', NULL, timestamptz '2002-10-30T23:48:14-05:00'),
    ('20021020103906Z263956-32597+5752@vger.kernel.org', NULL, timestamptz '2002-10-20T06:39:07-04:00'),
    ('3de379c5.5508.0@wincom.net', NULL, timestamptz '2002-11-26T08:19:17-05:00'),
    ('3de37b23.594a.0@wincom.net', NULL, timestamptz '2002-11-26T08:25:07-05:00'),
    ('3de992cb.2c55.0@wincom.net', NULL, timestamptz '2002-11-30T23:33:37-05:00'),
    ('20021105174158Z264972-32597+16136@vger.kernel.org', NULL, timestamptz '2002-11-05T12:41:59-05:00'),
    ('20021108184743.C0CE728475@mx02.ibest.com.br', NULL, timestamptz '2002-11-08T13:41:34-05:00'),
    ('3dea3f71.59f5.0@wincom.net', NULL, timestamptz '2002-12-01T11:50:13-05:00'),
    ('3dec4aff.50ae.0@wincom.net', NULL, timestamptz '2002-12-03T01:04:06-05:00'),
    ('20030227231043Z267196-29901+5334@vger.kernel.org', NULL, timestamptz '2003-02-27T18:10:44-05:00'),
    ('20030224015615Z269077-29901+1660@vger.kernel.org', NULL, timestamptz '2003-02-23T20:56:17-05:00'),
    ('20030314114442Z262599-25575+29962@vger.kernel.org', NULL, timestamptz '2003-03-14T06:44:43-05:00'),
    ('S263875AbTDYUZs/20030425202548Z+5150@vger.kernel.org', NULL, timestamptz '2003-04-25T16:25:52-04:00'),
    ('687.56569.436934@unknown', NULL, timestamptz '2003-05-13T23:25:00-04:00'),
    ('239.729672.35130@unknown', NULL, timestamptz '2003-05-19T03:59:35-04:00'),
    ('S263340AbTEOFhC/20030515053702Z+9528@vger.kernel.org', NULL, timestamptz '2003-05-15T01:37:02-04:00'),
    ('200306100815.h5A8FL613714@zeus.kernel.org', NULL, timestamptz '2003-06-10T04:01:43-04:00'),
    ('S270432AbTGWQ2b/20030723162831Z+7002@vger.kernel.org', NULL, timestamptz '2003-07-23T12:28:32-04:00'),
    ('200308221006.h7MA69619589@zeus.kernel.org', NULL, timestamptz '2003-08-22T06:06:13-04:00'),
    ('S262307AbTJJUFZ/20031010200525Z+2111@vger.kernel.org', NULL, timestamptz '2003-10-10T16:05:27-04:00'),
    ('200311151421.hAFELAK27985@zeus.kernel.org', NULL, timestamptz '2003-11-15T09:21:14-05:00'),
    ('S263812AbTK2RXr/20031129172347Z+13438@vger.kernel.org', NULL, timestamptz '2003-11-29T12:23:50-05:00'),
    ('20031223054115.32A791E030CA3@csbd.org', NULL, timestamptz '2003-12-23T00:48:14-05:00'),
    ('200401032224.mail.0@mail.tin.it', NULL, timestamptz '2004-01-03T16:23:51-05:00'),
    ('200404181650.i3IGoeA10345@mail.fjpn.com.br', NULL, timestamptz '2004-04-18T13:17:07-04:00'),
    ('S264432AbUDZFZx/20040426052553Z+2635@vger.kernel.org', NULL, timestamptz '2004-04-26T01:25:59-04:00'),
    ('200405041046.i44AkGA20476@ken.astraware.co.uk', NULL, timestamptz '2004-05-04T06:46:29-04:00'),
    ('S264861AbUEPCFM/20040516020512Z+8468@vger.kernel.org', NULL, timestamptz '2004-05-15T22:05:21-04:00'),
    ('200405161354.i4GDsMq21474@zeus.kernel.org', NULL, timestamptz '2004-05-16T09:54:27-04:00'),
    ('200405231108.i4NB8QGA017862@tirith.eregion.local', NULL, timestamptz '2004-05-23T07:10:20-04:00'),
    ('S264884AbUFAEvU/20040601045120Z+911@vger.kernel.org', NULL, timestamptz '2004-06-01T00:51:20-04:00'),
    ('S264662AbUFAHRT/20040601071719Z+1063@vger.kernel.org', NULL, timestamptz '2004-06-01T03:17:23-04:00'),
    ('200406051409.i55E3fUB021539@proinfo.com.uy', NULL, timestamptz '2004-06-05T10:05:54-04:00'),
    ('200406062020.i56KKOO8016825@zeus.kernel.org', NULL, timestamptz '2004-06-06T16:21:08-04:00'),
    ('S263984AbUFKO0t/20040611142649Z+718@vger.kernel.org', NULL, timestamptz '2004-06-11T10:26:50-04:00'),
    ('x889261780.9936648127512786546@oembqmxcg', NULL, timestamptz '2004-06-11T19:19:38-04:00'),
    ('x721288276.7525293151102563149@sjfehmluc', NULL, timestamptz '2004-06-13T09:00:30-04:00'),
    ('427.157377.86072@mx2.hotmail.com', NULL, timestamptz '2004-06-21T06:28:12-04:00'),
    ('x517849832.2428152063619342081@duiqrjloq', NULL, timestamptz '2004-06-25T06:54:48-04:00'),
    ('S266516AbUGKIUp/20040711082045Z+831@vger.kernel.org', NULL, timestamptz '2004-07-11T04:20:52-04:00'),
    ('x012969798.6035742114852714190@rbfnudavo', NULL, timestamptz '2004-07-14T10:50:17-04:00'),
    ('S266508AbUGPJ3N/20040716092913Z+1328@vger.kernel.org', NULL, timestamptz '2004-07-16T05:29:37-04:00'),
    ('20040718113823.5D26F41ED9@zeus.keynet.com.uy', NULL, timestamptz '2004-07-18T10:34:22-04:00'),
    ('S266181AbUGZPkw/20040726154052Z+163@vger.kernel.org', NULL, timestamptz '2004-07-26T12:49:38-04:00'),
    ('200407261958.i6QJw8VD004845@smail3.alcatel.fr', NULL, timestamptz '2004-07-26T16:33:37-04:00'),
    ('S266494AbUHOFSB/20040815051801Z+298@vger.kernel.org', NULL, timestamptz '2004-08-15T01:18:13-04:00'),
    ('200408260656.i7Q6uOcv001939@mail5.fullerton.edu', NULL, timestamptz '2004-08-26T02:36:08-04:00'),
    ('200408080518.i785I8X5006859@zeus.kernel.org', NULL, timestamptz '2004-08-08T01:18:26-04:00'),
    ('E1CaFo0-00023M-FA@taipei.dattaweb.com', NULL, timestamptz '2004-12-03T11:14:49-05:00'),
    ('E1CaFnN-0001ym-Fa@taipei.dattaweb.com', NULL, timestamptz '2004-12-03T11:15:52-05:00'),
    ('145.59909.562166@gtlaser.com', NULL, timestamptz '2005-01-13T10:11:05-05:00'),
    ('878y0i930w.fsf@barad-dur.minas-morgul.org', NULL, timestamptz '2005-07-08T09:11:44-04:00'),
    ('200508280306.mail.0@ganeyaviv.com', NULL, timestamptz '2005-08-27T20:05:55-04:00'),
    ('6969606887.20051223043926@0451.com', NULL, timestamptz '2005-12-22T23:39:30-05:00'),
    ('p06210202beea43ef060b@[192.168.1.96]', NULL, timestamptz '2006-04-27T23:10:19-04:00'),
    ('200605090732.k497VwFM014476@zeus1.kernel.org', NULL, timestamptz '2006-05-09T03:32:13-04:00'),
    ('200608201419.k7KEJbm2005461@zeus1.kernel.org', NULL, timestamptz '2006-08-20T10:19:56-04:00'),
    ('20081010122108.35784D1C02B@perso1.free.fr', NULL, timestamptz '2008-10-10T08:21:17-04:00'),
    ('20081010122839.97746D1BA32@perso1.free.fr', NULL, timestamptz '2008-10-10T08:28:40-04:00'),
    ('20081016111213.57575DDFA2@ozlabs.org', NULL, timestamptz '2008-10-16T07:12:25-04:00'),
    ('20081016111212.6F127DDFA1@ozlabs.org', NULL, timestamptz '2008-10-16T07:12:38-04:00'),
    ('20081016111214.417B8DDFAF@ozlabs.org', NULL, timestamptz '2008-10-16T07:12:52-04:00'),
    ('20081016111213.E77C3DDFA3@ozlabs.org', NULL, timestamptz '2008-10-16T07:13:16-04:00'),
    ('20081016111214.717CDDDFB0@ozlabs.org', NULL, timestamptz '2008-10-16T07:13:31-04:00'),
    ('20081023005750.43BBCDDEF4@ozlabs.org', NULL, timestamptz '2008-10-22T20:58:03-04:00'),
    ('20081023005750.72754DDEF7@ozlabs.org', NULL, timestamptz '2008-10-22T20:58:24-04:00'),
    ('20081023005749.F1E89DDED6@ozlabs.org', NULL, timestamptz '2008-10-22T20:58:42-04:00'),
    ('20081023005750.A1F5DDDEF8@ozlabs.org', NULL, timestamptz '2008-10-22T20:58:56-04:00'),
    ('20081023005751.0AE1FDDEFB@ozlabs.org', NULL, timestamptz '2008-10-22T21:00:05-04:00'),
    ('20081117132509.361BEDDD0B@ozlabs.org', NULL, timestamptz '2008-11-17T08:25:25-05:00'),
    ('20081117132509.757C5DDDE6@ozlabs.org', NULL, timestamptz '2008-11-17T08:25:42-05:00'),
    ('20081117132509.AB4B3DDDF5@ozlabs.org', NULL, timestamptz '2008-11-17T08:25:57-05:00'),
    ('20081117132630.33F09DDDF5@ozlabs.org', NULL, timestamptz '2008-11-17T08:26:42-05:00'),
    ('20081117132630.6E302DDDFB@ozlabs.org', NULL, timestamptz '2008-11-17T08:27:00-05:00'),
    ('20081117132630.E2265DDE00@ozlabs.org', NULL, timestamptz '2008-11-17T08:27:30-05:00'),
    ('20081117132631.3F127DDE04@ozlabs.org', NULL, timestamptz '2008-11-17T08:27:47-05:00'),
    ('20081117132631.76367DDE07@ozlabs.org', NULL, timestamptz '2008-11-17T08:28:29-05:00'),
    ('20081117132631.A88A0DDE0A@ozlabs.org', NULL, timestamptz '2008-11-17T08:28:46-05:00'),
    ('TbjqLMBE7AccP8AizRWaR', NULL, timestamptz '2009-06-01T19:32:00-04:00'),
    ('w3bTOcFVZeKLmp032HcjFA', NULL, timestamptz '2009-06-04T11:19:01-04:00'),
    ('1335961324.c7049afee5c1e6546c1b7872b5db8df1@ibai-institut-newsletter.de', NULL, timestamptz '2012-05-02T08:22:06-04:00'),
    ('1349706837.1f90ec0585b0c0078e38c1c43c74b896@ibai-institut-newsletter.de', NULL, timestamptz '2012-10-08T10:44:00-04:00'),
    ('20121008150228.C1DE62336E@deniol129.nionex.net', NULL, timestamptz '2012-10-08T11:02:32-04:00'),
    ('1349774270.1f90ec0585b0c0078e38c1c43c74b896@ibai-institut-newsletter.de', NULL, timestamptz '2012-10-09T05:17:57-04:00'),
    ('20121022160900.47198AA11A@deniol129.nionex.net', NULL, timestamptz '2012-10-22T12:09:05-04:00'),
    ('55281F46.9090402@asla.ca', NULL, timestamptz '2015-04-10T06:06:36-04:00'),
    ('20260812071538.o7mdtMhZ1eOzDgQnKZduQfWlq0FhL7OCAPLjt9AA11w@z', NULL, timestamptz '2026-08-12T07:15:38+00:00'),
    ('tencent_72A978D4934F50D46D2ACFF7D70896938806@qq.com', NULL, timestamptz '2026-08-27T16:01:30+00:00'),
    ('tencent_2821C8E70DC11122A717BA875978E4ADC005@qq.com', NULL, timestamptz '2026-08-30T09:35:51+00:00');

DO $$
DECLARE
    matching_rows integer;
BEGIN
    SELECT count(*)
    INTO matching_rows
    FROM messages AS message
    JOIN message_timestamp_repairs AS repair
      ON repair.message_id = message.message_id
     AND message.sent_at IS NOT DISTINCT FROM repair.old_sent_at;

    IF matching_rows <> 238 THEN
        RAISE EXCEPTION
            'Expected 238 unchanged malformed messages, found %',
            matching_rows;
    END IF;

    IF (
        SELECT count(*)
        FROM threads
        WHERE root_message_id IN (
            '20320324042550601563@xaceg.cn',
            '20320424032957531005@xaceg.cn'
        )
    ) <> 2 THEN
        RAISE EXCEPTION 'Expected both xaceg spam threads to exist';
    END IF;
END
$$;

CREATE TEMP TABLE affected_threads (
    thread_id bigint PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO affected_threads (thread_id)
SELECT DISTINCT message.thread_id
FROM messages AS message
JOIN message_timestamp_repairs AS repair
  ON repair.message_id = message.message_id;

-- These four unresolved parent placeholders inherited bogus child dates.
-- NULL accurately represents that their original messages were never seen.
UPDATE messages
SET sent_at = NULL,
    updated_at = now()
WHERE is_placeholder
  AND sent_at < timestamptz '1991-01-01';

UPDATE messages AS message
SET sent_at = repair.corrected_sent_at,
    updated_at = now()
FROM message_timestamp_repairs AS repair
WHERE message.message_id = repair.message_id
  AND message.sent_at IS NOT DISTINCT FROM repair.old_sent_at;

-- Keep patch-series dates aligned with the corrected source message. Prefer a
-- cover letter; otherwise use the earliest corrected diff-bearing part.
WITH patchset_corrections AS (
    SELECT
        patchset.id,
        COALESCE(
            cover_repair.corrected_sent_at,
            min(part_repair.corrected_sent_at)
        ) AS corrected_sent_at
    FROM patchsets AS patchset
    LEFT JOIN message_timestamp_repairs AS cover_repair
      ON cover_repair.message_id = patchset.cover_letter_message_id
    LEFT JOIN patches AS patch
      ON patch.patchset_id = patchset.id
    LEFT JOIN message_timestamp_repairs AS part_repair
      ON part_repair.message_id = patch.message_id
    WHERE patchset.sent_at < timestamptz '1991-01-01'
       OR patchset.sent_at > now() + interval '7 days'
    GROUP BY patchset.id, cover_repair.corrected_sent_at
)
UPDATE patchsets AS patchset
SET sent_at = correction.corrected_sent_at,
    updated_at = now()
FROM patchset_corrections AS correction
WHERE patchset.id = correction.id
  AND correction.corrected_sent_at IS NOT NULL;

-- Rebuild the API's lastActivityAt value from the now-correct message dates.
UPDATE threads AS thread
SET last_updated_at = metadata.last_updated_at
FROM (
    SELECT
        affected.thread_id,
        COALESCE(
            max(COALESCE(message.sent_at, message.created_at))
                FILTER (WHERE NOT message.is_placeholder),
            target.created_at
        ) AS last_updated_at
    FROM affected_threads AS affected
    JOIN threads AS target
      ON target.id = affected.thread_id
    LEFT JOIN messages AS message
      ON message.thread_id = affected.thread_id
    GROUP BY affected.thread_id, target.created_at
) AS metadata
WHERE thread.id = metadata.thread_id;

-- Deleting a thread cascades through messages, patch data, lineage data, and
-- thread_search_documents. Restrict deletion to the two reviewed spam roots.
DELETE FROM threads
WHERE root_message_id IN (
    '20320324042550601563@xaceg.cn',
    '20320424032957531005@xaceg.cn'
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM messages AS message
        JOIN message_timestamp_repairs AS repair
          ON repair.message_id = message.message_id
        WHERE message.sent_at <> repair.corrected_sent_at
    ) THEN
        RAISE EXCEPTION 'One or more message timestamp repairs did not apply';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM messages
        WHERE lower(COALESCE(author, '')) LIKE '%test@xaceg.cn%'
    ) THEN
        RAISE EXCEPTION 'One or more xaceg spam messages remain';
    END IF;
END
$$;

COMMIT;
