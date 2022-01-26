import axios, { AxiosInstance } from 'axios';
import MomentTimezone from 'moment-timezone';
import { ServiceConfig } from './api.config'
import { Moment } from 'moment-timezone';
import { ApplicationEvent } from '../redux/Actions';
import { ImageKeyDto, ExchangedImageDTO, toJsonExchangedImageDTO } from '../model/DataModel';
import { Console } from 'console';
import { Observable } from 'rxjs';
var NDJsonRxJS = require('ndjson-rxjs');


export interface ImagesService {

    getNextImage(url: string | undefined): Promise<string>;
    getPrevImage(url: string | undefined): Promise<string>;
    getImage(url: string | undefined): Promise<string>;
    saveImage(url: string, img: ExchangedImageDTO): Promise<string>;
    checkout(img: ExchangedImageDTO): Promise<string>;
    delete(img: ExchangedImageDTO): Promise<string>;
    getLastImages(pageNumber: number): Promise<ReadableStream>;

    getPageOfImages(url?: string): Promise<ReadableStream>;

}
export default class ImagesServiceImpl implements ImagesService {

    axiosInstance: AxiosInstance;
    baseUrlToGetImagesPerDate: string;
    urlToGetLastImages: string;


    protected buildRow(rowName: string, creationDate: Moment, version: number, imageId: string): string {
        var params: { [key: string]: string } = {
            creationDate: encodeURIComponent(creationDate.format('YYYY-MM-DD HH:mm:ss +01')),
            version: encodeURIComponent(version),
            imageId: encodeURIComponent(imageId)
        };
        var row = Object.keys(params).map(key => key + ',' + params[key]).join(',');
        return rowName + '=' + row;
    }

    protected buildParam(paramName: string, paramValue: string) {
        return paramName + '=' + encodeURIComponent(paramValue);
    }

    protected buildSort(sorted: string, unsorted: string, empty: string) {
        var params: { [key: string]: string } = {
            sorted: sorted,
            unsorted: unsorted,
            empty: empty
        };
        var row = Object.keys(params).map(key => key + ',' + encodeURIComponent(params[key])).join(',');
        return 'sort=' + row;
    }

    protected buildURLToGetDates(min: Moment, max: Moment, intervalType: string): string {
        var getParams: { [key: string]: string } = {
            firstRow: this.buildRow('firstRow', MomentTimezone(0), 0, ' '),
            lastRow: this.buildRow('lastRow', MomentTimezone(0), 0, ' '),
            pageNumber: this.buildParam('pageNumber', '0'),
            size: this.buildParam('size', '100'),
            sort: this.buildSort('true', 'true', 'true'),
            offset: this.buildParam('offset', '100'),
            paged: this.buildParam('paged', 'true'),
            unpaged: this.buildParam('paged', 'true')
        };

        var queryParams = Object.keys(getParams).map(key => getParams[key]).join('&');

        var params: { [key: string]: string } = {
            intervalType: intervalType,
            firstDate: encodeURIComponent(min.format('YYYY-MM-DD HH:mm:ss +01')),
            lastDate: encodeURIComponent(max.format('YYYY-MM-DD HH:mm:ss +01')),
            version: '0'
        };
        var queryString = Object.keys(params).map(key => params[key]).join('/');
        return this.baseUrlToGetImagesPerDate + queryString + '?' + queryParams;
    }

    protected buildURLToGetLastImages(pageNumber: number) {
        var getParams: { [key: string]: string } = {
            firstRow: this.buildRow('firstRow', MomentTimezone(0), 0, ' '),
            lastRow: this.buildRow('lastRow', MomentTimezone(0), 0, ' '),
            pageNumber: this.buildParam('pageNumber', pageNumber.toString()),
            page: this.buildParam('page', pageNumber.toString()),
            size: this.buildParam('size', '100'),
            sort: this.buildSort('true', 'true', 'true'),
            offset: this.buildParam('offset', '100'),
            paged: this.buildParam('paged', 'true'),
            unpaged: this.buildParam('paged', 'true')
        };

        var queryParams = Object.keys(getParams).map(key => getParams[key]).join('&');
        return this.urlToGetLastImages + '?' + queryParams;

    }


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlToGetImagesPerDate = '/api/gs/images/odt/';
        this.urlToGetLastImages = '/api/gs/images';
    }

    async checkout(img: ExchangedImageDTO): Promise<string> {
        const url = img._links?._checkout?.href;
        if (url != null) {
            return this.axiosInstance.post(url, {
                headers: {
                    'Content-Type': 'application/json'
                }
            }).then(resp => resp.data);
        }
        return '';
    }

    async delete(img: ExchangedImageDTO): Promise<string> {
        const url = img._links?._del?.href;
        if (url != null) {
            return this.axiosInstance.post(url, {
                headers: {
                    'Content-Type': 'application/json'
                }
            }).then(resp => resp.data);
        }
        return '';
    }

    getString(r: any): string {
        return r != null ? r : ''
    }

    async saveImage(url: string, img: ExchangedImageDTO): Promise<string> {
        return this.axiosInstance.post(url, toJsonExchangedImageDTO(img), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }


    async getLastImages(pageNumber: number): Promise<ReadableStream> {
        const url = this.buildURLToGetLastImages(pageNumber);
        console.log("Calling url to get last images : " + url);
        const reponse: Observable<string> = await NDJsonRxJS.stream(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/stream+json',
                'Accept': 'application/stream+json'
            }
        });

        return new ReadableStream({
            start(controller) {
                pump();
                async function pump() {
                    reponse.subscribe({
                        next(v: string): void {
                            controller.enqueue(v);
                        },
                        complete(): void {
                            controller.close();
                        }

                    })
                }
            }
        });
    }

    async getPageOfImages(url: string): Promise<ReadableStream> {

        console.log("Calling url to get last images : " + url);
        const reponse: Observable<string> = await NDJsonRxJS.stream(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/stream+json',
                'Accept': 'application/stream+json'
            }
        });

        return new ReadableStream({
            start(controller) {
                pump();
                async function pump() {
                    reponse.subscribe({
                        next(v: string): void {
                            controller.enqueue(v);
                        },
                        complete(): void {
                            controller.close();
                        }

                    })
                }
            }
        });
    }

    async getNextImage(url: string): Promise<string> {
        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }
    async getPrevImage(url: string): Promise<string> {
        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }

    async getImage(url: string): Promise<string> {


        return this.axiosInstance.get(url)
            .then(resp => resp.data);
    }


}
