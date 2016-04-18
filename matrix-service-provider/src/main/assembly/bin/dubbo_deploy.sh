#/bin/bash

# variable define
package=/root/matrix-service-provider-1.0-assembly.tar.gz
projectname=matrix-service-provider-1.0
outfile=/opt/dubbo
replacefile=/home/admin/files

PIDS=`ps -ef | grep java | grep "${projectname}" |awk '{print $2}'`
if [ "$PIDS" ]; then
    echo "The $projectname are starting, next stoping it"
    ${outfile}/${projectname}/bin/stop.sh
    sleep 5s
fi

# unzip
tar -xzmf ${package} -C ${outfile}
echo '... unzip succ: '${package}

# replace file -- dubbo.properties
#cd $replacefile
#cp dubbo.properties ${outfile}/${projectname}/conf/dubbo.properties
#echo '... replace succ: '${outfile}'/'${projectname}'/conf/dubbo.properties'

# replace file -- ${projectname}.jar
#cd matrix-service-jar
#jar uf ${outfile}/${projectname}/lib/${projectname}.jar logback.xml
#jar uf ${outfile}/${projectname}/lib/${projectname}.jar m-dubbo/activemq.properties
#jar uf ${outfile}/${projectname}/lib/${projectname}.jar m-dubbo/jdbc.properties
#jar uf ${outfile}/${projectname}/lib/${projectname}.jar m-dubbo/url.properties
#jar uf ${outfile}/${projectname}/lib/${projectname}.jar m-dubbo/spring-dubbo.xml
#echo '... matrix-service-provider-1.0.jar replace succ'

echo '... ready successful, start server, execute: '${outfile}'/'${projectname}'/bin/start.sh'

echo "start matrix-service..."
cd ${outfile}/${projectname}/bin
dos2unix *
${outfile}/${projectname}/bin/start.sh
sleep 5s
tail -f ${outfile}/${projectname}/logs/stdout.log
exit