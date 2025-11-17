@param String[] logs

@if(logs == null || logs.length == 0)
    没有发现任何构建日志。
@else
    <table class="min-w-full divide-y divide-blue-500/20">
        <thead class="tech-gradient">
        <tr>
            <th class="px-3 lg:px-6 py-4 text-left text-xs font-medium text-blue-300 uppercase tracking-wider whitespace-nowrap">
                日志信息
            </th>
        </tr>
        </thead>
        <tbody class="bg-slate-800/50 divide-y divide-blue-500/10">
        @for(String log: logs)
            <tr class="hover:bg-blue-600/10 transition-colors duration-200">
                <td class="px-3 lg:px-6 py-4 whitespace-nowrap text-sm text-cyan-300 font-mono">${log}</td>
            </tr>
        @endfor
        </tbody>
    </table>
@endif

